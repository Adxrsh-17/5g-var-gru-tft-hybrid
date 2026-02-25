

import os
import io
import glob
import shutil
import subprocess
import warnings
import json
import argparse
import numpy as np
import pandas as pd
import tensorflow as tf
import tf2onnx
from tensorflow.keras import layers, models, callbacks, optimizers, losses, regularizers
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import mean_absolute_error, r2_score as sklearn_r2

try:
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False
    print("pyarrow not available -- HDFS streaming disabled")

warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

# =====================================================
# CONSTANTS -- locked to PcapKpiExtractor.scala (11 KPIs)
# These are the ONLY columns in the HDFS parquet files.
# =====================================================
FEATURE_COLUMNS = [
    "Throughput_bps",    # 0 -- primary KPI (forecast target)
    "Total_Packets",     # 1
    "Jitter_Variance",   # 2
    "Avg_Packet_Size",   # 3
    "Active_Flows",      # 4
    "TCP_Packets",       # 5
    "UDP_Packets",       # 6
    "Max_Packet_Size",   # 7
    "Min_Packet_Size",   # 8
    "TCP_Syn_Count",     # 9
    "TCP_Fin_Count",     # 10
]

# v5.0: Single-target output -- Throughput_bps traffic forecasting
TARGET_KPI     = "Throughput_bps"
NUM_FEATURES   = len(FEATURE_COLUMNS)       # 11 -- same as HDFS columns

# Target window: how many steps ahead to forecast (forward-looking avg)
TARGET_WINDOW_DEFAULT = 10   # mMTC
TARGET_WINDOW_HIGH    = 30   # eMBB + URLLC

# =====================================================
# DISTRIBUTED STRATEGY
# =====================================================
try:
    strategy = tf.distribute.MirroredStrategy()
    print(f"ACCELERATION: Running on {strategy.num_replicas_in_sync} GPU(s)")
except Exception:
    strategy = tf.distribute.get_strategy()
    print("ACCELERATION: Single Device (CPU)")

# =====================================================
# CUSTOM METRICS
# =====================================================
@tf.function
def r_squared(y_true, y_pred):
    """R2 for single-target output."""
    SS_res = tf.reduce_sum(tf.square(y_true - y_pred))
    SS_tot = tf.reduce_sum(tf.square(y_true - tf.reduce_mean(y_true)))
    return 1.0 - SS_res / (SS_tot + tf.keras.backend.epsilon())


# =====================================================
# CLI ARGUMENTS
# =====================================================
def parse_args():
    p = argparse.ArgumentParser(description="5G KPI v5.0 -- Pure Raw KPI Traffic Forecasting")
    p.add_argument("--data_dir", "--hdfs-path", type=str, default=None,
                   help="Parquet path: local dir or hdfs://namenode:8020/path")
    p.add_argument("--slice_name", "--slice", type=str, required=True,
                   choices=["eMBB", "URLLC", "mMTC"])
    p.add_argument("--output_dir", type=str, default="./artifacts")
    p.add_argument("--epochs",     type=int, default=300,
                   help="Max epochs -- EarlyStopping guards over-running")
    p.add_argument("--batch_size", "--batch-size", type=int, default=256)
    p.add_argument("--lr", "--learning-rate", type=float, default=0.001,
                   help="Learning rate (default 0.001 for v5.0)")
    args = p.parse_args()

    # If data_dir not given, auto-construct slice path
    if args.data_dir is None:
        args.data_dir = f"hdfs://namenode:8020/5g_kpi/processed/sliceType={args.slice_name}"
        print(f"   Auto data_dir -> {args.data_dir}")
    elif "/5g_kpi/processed" in args.data_dir and "sliceType=" not in args.data_dir:
        base = args.data_dir.rstrip("/")
        args.data_dir = f"{base}/sliceType={args.slice_name}"
        print(f"   Appended slice -> {args.data_dir}")

    if not args.output_dir.endswith(args.slice_name):
        args.output_dir = os.path.join(args.output_dir, args.slice_name)

    return args


# =====================================================
# DATA LOADING -- RAW 11 KPIs ONLY (no synthetic features)
# =====================================================
def load_and_prep_data(data_path: str, slice_name: str):
    """
    Loads parquet data from HDFS/local and uses ONLY the 11 raw
    KPI columns written by PcapKpiExtractor.scala.

    No rolling means, no rolling std, no diffs -- pure raw data.

    Returns:
        X : np.ndarray (N, 11) -- 11 raw KPI features
        y : np.ndarray (N, 1)  -- single-target Throughput_bps
    """
    print(f"\nLoading data for [{slice_name}] from: {data_path}")

    dfs = []

    # -- HDFS path -------------------------------------------------------
    if data_path.startswith("hdfs://"):
        if not HDFS_AVAILABLE:
            raise RuntimeError("pyarrow not installed -- cannot read HDFS")

        parts = data_path.replace("hdfs://", "").split("/", 1)
        host_port = parts[0]
        hdfs_path = "/" + parts[1] if len(parts) > 1 else "/"
        host = host_port.split(":")[0]
        port = int(host_port.split(":")[1]) if ":" in host_port else 8020

        print(f"   Connecting to HDFS: {host}:{port}")
        filesystem = pafs.HadoopFileSystem(host=host, port=port)

        try:
            file_info = filesystem.get_file_info(pafs.FileSelector(hdfs_path, recursive=True))
            parquet_files = [f.path for f in file_info
                            if f.type.name == "File" and f.path.endswith(".parquet")]
            print(f"   Found {len(parquet_files)} parquet files")

            for fpath in parquet_files:
                try:
                    table = pq.read_table(fpath, filesystem=filesystem)
                    df_tmp = table.to_pandas()

                    # Inject sliceType from HDFS partition folder
                    if "sliceType" not in df_tmp.columns:
                        for s in ["eMBB", "URLLC", "mMTC"]:
                            if f"sliceType={s}" in fpath or f"/{s}/" in fpath:
                                df_tmp["sliceType"] = s
                                break
                    dfs.append(df_tmp)

                except Exception as e:
                    print(f"   Skipped {os.path.basename(fpath)}: {e}")
        except Exception as e:
            raise RuntimeError(f"HDFS error: {e}")

    # -- Local path -------------------------------------------------------
    else:
        if not os.path.exists(data_path):
            alt_paths = [
                os.path.join("data", f"sliceType={slice_name}"),
                os.path.join("data", slice_name),
                "data",
            ]
            for alt in alt_paths:
                if os.path.exists(alt):
                    data_path = alt
                    print(f"   Redirected to: {data_path}")
                    break
            else:
                raise FileNotFoundError(f"No data at {data_path} or alternatives")

        parquet_files = glob.glob(os.path.join(data_path, "**/*.parquet"), recursive=True)
        if not parquet_files:
            parquet_files = glob.glob(os.path.join(data_path, "*.parquet"))
        print(f"   Found {len(parquet_files)} local parquet files")

        for fpath in parquet_files:
            try:
                df_tmp = pd.read_parquet(fpath)
                if "sliceType" not in df_tmp.columns:
                    for s in ["eMBB", "URLLC", "mMTC"]:
                        if f"sliceType={s}" in fpath or f"/{s}/" in fpath:
                            df_tmp["sliceType"] = s
                            break
                dfs.append(df_tmp)
            except Exception as e:
                print(f"   Skipped {os.path.basename(fpath)}: {e}")

    if not dfs:
        raise RuntimeError(f"No parquet files loaded from {data_path}")

    df_all = pd.concat(dfs, ignore_index=True)
    print(f"   Total rows loaded: {len(df_all):,}")

    # -- Filter to requested slice ----------------------------------------
    if "sliceType" in df_all.columns:
        df_slice = df_all[df_all["sliceType"] == slice_name].copy()
        print(f"   Filtered to {slice_name}: {len(df_slice):,} rows")
    else:
        df_slice = df_all.copy()
        print(f"   No sliceType column -- using all {len(df_slice):,} rows")

    if len(df_slice) < 200:
        raise RuntimeError(f"Only {len(df_slice)} rows for {slice_name} -- need >=200")

    # -- Verify all 11 feature columns exist ------------------------------
    missing = [c for c in FEATURE_COLUMNS if c not in df_slice.columns]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}")

    # -- Base 11-feature DataFrame ----------------------------------------
    df = df_slice[FEATURE_COLUMNS].copy()
    df = df.ffill().bfill().fillna(0)

    # Clip extreme outliers per column (1st-99th percentile)
    for col in FEATURE_COLUMNS:
        lo = df[col].quantile(0.01)
        hi = df[col].quantile(0.99)
        if hi > lo:
            df[col] = df[col].clip(lower=lo, upper=hi)
    print(f"   Clipped outliers to [1st, 99th] percentile per KPI")

    # v5.0: Use raw 11 columns directly -- no synthetic feature engineering
    print(f"   Using raw {NUM_FEATURES} KPIs directly (no rolling/std/diff)")

    # -- Target creation: single-target, slice-specific window ------------
    if slice_name in ("eMBB", "URLLC"):
        target_window = TARGET_WINDOW_HIGH
    else:
        target_window = TARGET_WINDOW_DEFAULT

    print(f"   Creating {target_window}-step forward smoothed target: {TARGET_KPI}")

    target_col = f"Target_{TARGET_KPI}"
    if target_window == 1:
        df[target_col] = df[TARGET_KPI].shift(-1)
    else:
        df[target_col] = (
            df[TARGET_KPI].rolling(window=target_window).mean()
                          .shift(-(target_window - 1))
        )

    # Drop NaN rows from target shift
    df = df.dropna().reset_index(drop=True)

    X = df[FEATURE_COLUMNS].values.astype(np.float32)    # (N, 11)
    y = df[[target_col]].values.astype(np.float32)        # (N, 1)

    print(f"   Ready: {X.shape[0]:,} samples | X={X.shape} | y={y.shape}")
    print(f"   Target window: {target_window} steps | Target: {TARGET_KPI}")
    return X, y, target_window


# =====================================================
# MODEL ARCHITECTURE -- Gated Residual Network v4.0
# =====================================================
class GatedResidualNetwork(layers.Layer):
    """
    Gated Residual Network block with optional projection.
    Gate mechanism learns to suppress redundant features.
    """
    def __init__(self, units, dropout_rate=0.1, **kwargs):
        super().__init__(**kwargs)
        self.units = units
        self.dropout_rate = dropout_rate

    def build(self, input_shape):
        input_dim = int(input_shape[-1])
        self.dense1 = layers.Dense(self.units, activation="elu",
                                    kernel_regularizer=regularizers.l2(1e-4))
        self.dense2 = layers.Dense(self.units,
                                    kernel_regularizer=regularizers.l2(1e-4))
        self.gate   = layers.Dense(self.units, activation="sigmoid")
        self.bn     = layers.BatchNormalization()
        self.dropout = layers.Dropout(self.dropout_rate)
        if input_dim != self.units:
            self.proj = layers.Dense(self.units, use_bias=False)
        else:
            self.proj = None
        super().build(input_shape)

    def call(self, x, training=False):
        residual = self.proj(x) if self.proj else x
        h = self.dense1(x)
        h = self.dropout(h, training=training)
        h = self.dense2(h)
        g = self.gate(x)
        h = g * h + (1 - g) * residual
        h = self.bn(h, training=training)
        return h

    def get_config(self):
        config = super().get_config()
        config.update({"units": self.units, "dropout_rate": self.dropout_rate})
        return config


def build_model(slice_name: str, lr: float = 0.001):
    """
    v5.0 GRN-Dense Network -- 11-feature raw input, 1-target output.

    All slices use the same compact architecture:
      Input(11) -> InputBN -> GRN(256) -> GRN(128) -> GRN(64) ->
      Dense(32) -> Dense(1)
    """
    inp = layers.Input(shape=(NUM_FEATURES,), name="kpi_input")

    # Input BatchNorm -- normalises the 11 raw KPIs
    x = layers.BatchNormalization(name="input_bn")(inp)

    # v5.0: Compact GRN stack for 11-dim raw input
    print(f"   {slice_name}: GRN stack (256 -> 128 -> 64) for 11-dim raw input")
    x = GatedResidualNetwork(256, dropout_rate=0.15, name="grn_256")(x)
    x = GatedResidualNetwork(128, dropout_rate=0.10, name="grn_128")(x)
    x = GatedResidualNetwork(64,  dropout_rate=0.05, name="grn_64")(x)

    x = layers.Dense(32, activation="relu",
                     kernel_regularizer=regularizers.l2(1e-4),
                     name="pre_output")(x)

    # Single-target output -- Throughput_bps
    out = layers.Dense(1, activation="linear", name="output")(x)

    model = models.Model(inputs=inp, outputs=out, name=f"GRN_{slice_name}_v5")

    print(f"   Input : (batch, {NUM_FEATURES}) -- 11 raw KPIs from HDFS")
    print(f"   Output: (batch, 1) -- {TARGET_KPI} forecast")

    # Huber loss (d=1.0) -- robust to outliers
    loss_fn = tf.keras.losses.Huber(delta=1.0)
    print(f"   Loss: Huber (d=1.0)")

    # v5.0: Adam with LR=0.001 (proven for compact models)
    opt = optimizers.Adam(learning_rate=lr, clipnorm=0.5)
    print(f"   Optimizer: Adam(lr={lr}, clipnorm=0.5)")

    model.compile(
        optimizer=opt,
        loss=loss_fn,
        metrics=["mae", r_squared]
    )
    model.summary(line_length=90)
    return model


# =====================================================
# REAL-WORLD EVALUATION
# =====================================================
def evaluate_real_world(model, X_test, y_test, scaler_target, slice_name):
    """
    Computes MAE and R2 in real physical units (bps) by inverting
    RobustScaler on the single-target Throughput_bps predictions.
    """
    print("\nComputing real-world metrics (inverse-transformed)...")

    preds_scaled = model.predict(X_test, verbose=0)   # (N, 1) scaled

    preds_real = scaler_target.inverse_transform(preds_scaled)
    y_real     = scaler_target.inverse_transform(y_test)

    # Ensure non-negative
    preds_real = np.maximum(preds_real, 0)
    y_real     = np.maximum(y_real, 0)

    tp_real = y_real[:, 0]
    tp_pred = preds_real[:, 0]
    mae_bps  = mean_absolute_error(tp_real, tp_pred)
    mean_bps = np.mean(tp_real)
    r2       = sklearn_r2(tp_real, tp_pred)
    acc      = max(0, (1 - mae_bps / (mean_bps + 1e-9)) * 100)

    print(f"\n{'='*55}")
    print(f"  REAL-WORLD VERDICT -- {slice_name} (v5.0)")
    print(f"{'='*55}")
    print(f"  Target KPI     : {TARGET_KPI}")
    print(f"  Throughput MAE : {mae_bps:>15,.0f} bps  ({mae_bps/1e6:.3f} Mbps)")
    print(f"  Mean Throughput: {mean_bps:>15,.0f} bps")
    print(f"  Estimated Acc. : {acc:>14.2f} %")
    print(f"  R2 Score       : {r2:>14.4f}")
    print(f"{'='*55}")
    verdict = ("EXCELLENT"    if r2 >= 0.85 else
               "GOOD"         if r2 >= 0.65 else
               "ACCEPTABLE"   if r2 >= 0.40 else
               "POOR -- needs more data or architecture changes")
    print(f"  Verdict: {verdict}")
    print(f"{'='*55}")

    return {
        "target_kpi":            TARGET_KPI,
        "throughput_mae_bps":    float(mae_bps),
        "throughput_r2":         float(r2),
        "estimated_accuracy":    float(acc),
        "mean_throughput_bps":   float(mean_bps),
        "verdict":               verdict,
    }


# =====================================================
# ONNX EXPORT
# =====================================================
def export_onnx(model, output_dir):
    """Export Keras model to ONNX using tf2onnx.convert.from_keras."""
    print("\nExporting ONNX (from_keras)...")
    onnx_path = os.path.join(output_dir, "model.onnx")
    try:
        input_spec = (tf.TensorSpec((None, NUM_FEATURES), tf.float32, name="kpi_input"),)
        model_proto, _ = tf2onnx.convert.from_keras(model, input_signature=input_spec,
                                                      opset=13, output_path=onnx_path)
        size_mb = os.path.getsize(onnx_path) / (1024 * 1024)
        print(f"   ONNX saved -> {onnx_path}  ({size_mb:.2f} MB)")
        return onnx_path
    except Exception as e:
        print(f"   ONNX export failed: {e}")
        try:
            print("   Retrying with subprocess tf2onnx CLI...")
            savedmodel_dir = os.path.join(output_dir, "_temp_savedmodel")
            model.save(savedmodel_dir, save_format="tf")
            cmd = [
                "python", "-m", "tf2onnx.convert",
                "--saved-model", savedmodel_dir,
                "--output", onnx_path,
                "--opset", "13"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if os.path.exists(savedmodel_dir):
                shutil.rmtree(savedmodel_dir, ignore_errors=True)
            if result.returncode == 0 and os.path.exists(onnx_path):
                size_mb = os.path.getsize(onnx_path) / (1024 * 1024)
                print(f"   ONNX saved (CLI) -> {onnx_path}  ({size_mb:.2f} MB)")
                return onnx_path
            else:
                print(f"   CLI export failed: {result.stderr}")
                return None
        except Exception as e2:
            print(f"   CLI fallback failed: {e2}")
            return None


# =====================================================
# HDFS UPLOAD
# =====================================================
def upload_to_hdfs(local_dir, slice_name):
    """Upload model artifacts to HDFS model repository."""
    if not HDFS_AVAILABLE:
        print("   pyarrow not available -- skipping HDFS upload")
        return False

    try:
        filesystem = pafs.HadoopFileSystem(host="namenode", port=8020)
        hdfs_base = f"/5g_kpi/models/{slice_name}"

        try:
            filesystem.create_dir(hdfs_base, recursive=True)
        except Exception:
            pass

        onnx_local = os.path.join(local_dir, "model.onnx")
        if os.path.exists(onnx_local):
            hdfs_onnx = f"{hdfs_base}/model_latest.onnx"
            with open(onnx_local, "rb") as f:
                data = f.read()
            with filesystem.open_output_stream(hdfs_onnx) as out:
                out.write(data)
            print(f"   ONNX -> hdfs://{hdfs_onnx}")

        scaler_local = os.path.join(local_dir, "scaler_params.json")
        if os.path.exists(scaler_local):
            hdfs_scaler = f"{hdfs_base}/scaler_latest.json"
            with open(scaler_local, "rb") as f:
                data = f.read()
            with filesystem.open_output_stream(hdfs_scaler) as out:
                out.write(data)
            print(f"   Scaler -> hdfs://{hdfs_scaler}")

        hist_local = os.path.join(local_dir, "history.json")
        if os.path.exists(hist_local):
            hdfs_hist = f"{hdfs_base}/history.json"
            with open(hist_local, "rb") as f:
                data = f.read()
            with filesystem.open_output_stream(hdfs_hist) as out:
                out.write(data)
            print(f"   History -> hdfs://{hdfs_hist}")

        print(f"   All artifacts uploaded for {slice_name}!")
        return True
    except Exception as e:
        print(f"   HDFS upload failed: {e}")
        return False


# =====================================================
# MAIN PIPELINE
# =====================================================
def run_pipeline(args):
    config = {
        "epochs":       args.epochs,
        "batch_size":   args.batch_size,
        "lr":           args.lr,
        "target_slice": args.slice_name,
    }

    # Slice-specific callback patience
    if args.slice_name in ("eMBB", "URLLC"):
        early_stop_patience = 40
        lr_patience = 10
    else:
        early_stop_patience = 25
        lr_patience = 7

    print("\n" + "="*65)
    print(f"  5G NWDAF v5.0 -- PURE RAW KPI TRAFFIC FORECASTING")
    print(f"  Slice : {args.slice_name}")
    print(f"  Mode  : GRN-Dense ({NUM_FEATURES}-feature raw, single-target)")
    print(f"  Data  : {args.data_dir}")
    print(f"  Out   : {args.output_dir}")
    print(f"  epochs={config['epochs']}  "
          f"batch={config['batch_size']}  lr={config['lr']}")
    print(f"  EarlyStopping patience: {early_stop_patience}")
    print("="*65 + "\n")

    os.makedirs(args.output_dir, exist_ok=True)

    # -- 1. Load raw 11-feature dataset from HDFS ------------------------
    X, y, target_window = load_and_prep_data(args.data_dir, args.slice_name)

    # -- 2. Temporal split (85/15 chronological) --------------------------
    split_idx = int(len(X) * 0.85)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    print(f"\nTemporal split: train={X_train.shape[0]:,} | test={X_test.shape[0]:,}")

    # -- 3. RobustScaler -- separate for input (11) and target (1) --------
    print("Fitting RobustScaler on training data...")
    scaler_input = RobustScaler()
    X_train = scaler_input.fit_transform(X_train)
    X_test  = scaler_input.transform(X_test)

    scaler_target = RobustScaler()
    y_train = scaler_target.fit_transform(y_train)
    y_test  = scaler_target.transform(y_test)

    X_train = X_train.astype(np.float32)
    X_test  = X_test.astype(np.float32)
    y_train = y_train.astype(np.float32)
    y_test  = y_test.astype(np.float32)

    print(f"   Input ({NUM_FEATURES}-feat) -- "
          f"Center(avg): {np.mean(scaler_input.center_):.4f} | "
          f"Scale(avg): {np.mean(scaler_input.scale_):.4f}")
    print(f"   Target (1-feat)   -- "
          f"Center: {scaler_target.center_[0]:.4f} | "
          f"Scale: {scaler_target.scale_[0]:.4f}")

    # -- 4. Build model ---------------------------------------------------
    print(f"\nBuilding v5.0 GRN model for {args.slice_name}...")
    with strategy.scope():
        model = build_model(args.slice_name, lr=config["lr"])

    # -- 5. Callbacks -- monitor val_r_squared ----------------------------
    ckpt_path = os.path.join(args.output_dir, "best_checkpoint.tf")
    cb_list = [
        callbacks.EarlyStopping(
            monitor="val_r_squared", mode="max",
            patience=early_stop_patience,
            restore_best_weights=True, verbose=1
        ),
        callbacks.ReduceLROnPlateau(
            monitor="val_r_squared", mode="max",
            factor=0.5, patience=lr_patience,
            min_lr=1e-6, verbose=1
        ),
        callbacks.ModelCheckpoint(
            filepath=ckpt_path, monitor="val_r_squared",
            mode="max", save_best_only=True, save_format="tf", verbose=0
        ),
        callbacks.CSVLogger(
            os.path.join(args.output_dir, "training_log.csv"), append=False
        )
    ]

    # -- 6. Train ---------------------------------------------------------
    print(f"\nTraining {args.slice_name} -- up to {config['epochs']} epochs "
          f"(EarlyStopping patience={early_stop_patience})...\n")
    history = model.fit(
        X_train, y_train,
        epochs=config["epochs"],
        batch_size=config["batch_size"] * strategy.num_replicas_in_sync,
        validation_data=(X_test, y_test),
        callbacks=cb_list,
        verbose=1
    )

    best_r2  = max(history.history.get("val_r_squared", [0]))
    best_mae = min(history.history.get("val_mae", [float("inf")]))
    print(f"\n   Best val R2  : {best_r2:.4f}")
    print(f"   Best val MAE : {best_mae:.6f} (scaled)")

    # -- 7. Real-world evaluation -----------------------------------------
    metrics = evaluate_real_world(model, X_test, y_test, scaler_target, args.slice_name)

    # -- 8. ONNX export ---------------------------------------------------
    onnx_path = export_onnx(model, args.output_dir)

    # -- 9. Save history --------------------------------------------------
    history_path = os.path.join(args.output_dir, "history.json")
    history_data = {k: [float(v) for v in vals] for k, vals in history.history.items()}
    with open(history_path, "w") as f:
        json.dump(history_data, f, indent=2)
    print(f"   History -> {history_path}")

    # -- 10. Save weights -------------------------------------------------
    weights_path = os.path.join(args.output_dir, "best.weights.h5")
    try:
        model.save_weights(weights_path)
        print(f"   Weights -> {weights_path}")
    except Exception as e:
        print(f"   Weights save failed: {e}")

    # -- 11. Scaler JSON -- 11 input + 1 output constants for Scala ------
    print("\nExporting scaler metadata (11-input / 1-output contract)...")
    scaler_data = {
        "slice_name":             args.slice_name,
        "architecture":           f"GRN_v5_{args.slice_name}",
        "version":                "5.0",
        "target_kpi":             TARGET_KPI,
        "target_steps_ahead":     target_window,

        # Feature name lists
        "input_features":         FEATURE_COLUMNS,     # 11 names
        "output_features":        [TARGET_KPI],         # 1 name

        # Dimensions
        "num_input_features":     NUM_FEATURES,         # 11
        "num_output_features":    1,

        # Input normalisation -- Scala applies BEFORE ONNX
        # normalised_input[i] = (raw_kpi[i] - center[i]) / scale[i]
        "center":           scaler_input.center_.tolist(),    # float[11]
        "scale":            scaler_input.scale_.tolist(),     # float[11]

        # Output inverse -- Scala applies AFTER ONNX returns prediction
        # throughput_bps = prediction * target_scale + target_center
        "target_center":    scaler_target.center_.tolist(),   # float[1]
        "target_scale":     scaler_target.scale_.tolist(),    # float[1]

        "onnx_exported":       onnx_path is not None,
        "training_metrics":    metrics,

        # Step-by-step Scala inference guide
        "_scala_inference_steps": [
            "1. Collect current second's 11 raw KPI values from PcapKpiExtractor",
            "2. norm_in[i]    = (raw_kpi[i] - center[i]) / scale[i]",
            "3. prediction    = ONNX.run(norm_in) -> float[1]",
            "4. throughput_bps = prediction[0] * target_scale[0] + target_center[0]",
        ]
    }

    scaler_path = os.path.join(args.output_dir, "scaler_params.json")
    with open(scaler_path, "w") as f:
        json.dump(scaler_data, f, indent=2)
    print(f"   Scaler JSON -> {scaler_path}")
    print(f"      center/scale      : {NUM_FEATURES} values (Scala input norm)")
    print(f"      target_center/scale : 1 value  (Scala output inverse)")

    # -- 12. HDFS upload --------------------------------------------------
    upload_to_hdfs(args.output_dir, args.slice_name)

    # -- 13. Final summary ------------------------------------------------
    print(f"\n{'='*65}")
    print(f"  v5.0 Pipeline complete [{args.slice_name}]")
    print(f"{'='*65}")
    print(f"   ONNX       : {onnx_path or 'FAILED'}")
    print(f"   JSON       : {scaler_path}")
    print(f"   Checkpoint : {ckpt_path}")
    print(f"   History    : {history_path}")
    print(f"   Best R2    : {best_r2:.4f}")
    print(f"   Features   : {NUM_FEATURES} raw KPIs (direct from HDFS)")
    print(f"   Target     : {TARGET_KPI} (traffic forecasting)")
    print(f"{'='*65}")

    return metrics


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    np.random.seed(42)
    tf.random.set_seed(42)
    args   = parse_args()
    result = run_pipeline(args)
    if result is None:
        exit(1)
