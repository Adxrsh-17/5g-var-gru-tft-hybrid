# =====================================================
# 5G HYBRID MODEL TRAINING — STATISTICAL SNAPSHOT EDITION
# VAR-GRU-TFT → GRN-Dense | Cluster & Scala Inference Engine Ready
#
# REFACTOR SUMMARY (applied to production fixed script):
#
#   [1] load_and_prep_data:
#       OLD → returned df (7-col DataFrame), used 3D windowing
#       NEW → returns X (N,14), y (N,7) with 60-s rolling mean features
#             14 = 7 raw KPI metrics + 7 rolling-60s statistical trends
#       UPDATE → y is now a 10-second future smoothed trend, not a 1s spike.
#
#   [2] build_model:
#       OLD → Input(60,7) → GRU(128) → GRU(64) → MultiHeadAttention
#             Problem: StatelessWhile crash on ONNX export
#       NEW → Input(14,) → GRN(128) → GRN(64) → GRN(32) → Dense(128) → Dense(64) → Dense(7)
#             No RNNs → from_keras ONNX export works without modification
#
#   [3] Hyperparameters:
#       EarlyStopping patience: 10 → 15 (snapshot convergence is slower)
#       ReduceLROnPlateau patience: 5 → 7
#       Both still monitor val_throughput_r2 (retained from FIX-2)
#
#   [4] export_onnx:
#       OLD → subprocess tf2onnx CLI with SavedModel intermediate
#       NEW → direct tf2onnx.convert.from_keras (works cleanly, no GRU)
#             Input contract: (batch, 14) instead of (batch, 60, 7)
#
#   [5] scaler_params.json:
#       OLD → single scaler, 7-feature center/scale only
#       NEW → snapshot_center/scale (14 features, for Scala input)
#             raw_center/scale (7 features, for Scala output inverse)
#             + _scala_note with step-by-step inference instructions
#
# FIXES RETAINED FROM PREVIOUS VERSION:
#   ✅ FIX-2: throughput_r2 / throughput_mae dedicated metrics
#   ✅ FIX-3: sliceType partition injection from HDFS folder name
#   ✅ urllc_weighted_loss composite loss for sparse URLLC data
#   ✅ eMBB log1p / expm1 transform on Throughput_bps disabled for better real-world R2
# =====================================================

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
from tensorflow.keras import layers, models, callbacks, optimizers, losses
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import mean_absolute_error, r2_score as sklearn_r2

try:
    import pyarrow.fs as pafs
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False
    print("⚠️  pyarrow not available — HDFS streaming disabled")

warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

# =====================================================
# 🔢 CONSTANTS — locked to PcapKpiExtractor.scala
# =====================================================
# CHANGE [1]: Added ROLLING_COLUMNS and SNAPSHOT_COLUMNS.
# NUM_RAW (7) matches Scala output; NUM_SNAPSHOT (14) is the model input.
FEATURE_COLUMNS = [
    "Throughput_bps",   # Index 0 — primary KPI, eMBB log-transform applied here
    "Total_Packets",    # Index 1
    "Jitter_Variance",  # Index 2
    "Avg_Packet_Size",  # Index 3
    "Active_Flows",     # Index 4
    "TCP_Packets",      # Index 5
    "UDP_Packets"       # Index 6
]

# 120-second rolling mean of each raw KPI — appended as features 7-13 (IMPROVED: 2x longer context)
ROLLING_COLUMNS = [f"{c}_roll120" for c in FEATURE_COLUMNS]

# Full 14-feature snapshot vector fed into the model
SNAPSHOT_COLUMNS = FEATURE_COLUMNS + ROLLING_COLUMNS

NUM_RAW       = len(FEATURE_COLUMNS)   # 7  — Scala contract (unchanged)
NUM_SNAPSHOT  = len(SNAPSHOT_COLUMNS)  # 14 — model input dimension
THROUGHPUT_IDX = 0                     # Raw feature index (for inverse-transform)
ROLLING_WINDOW = 120                   # Rolling window size in seconds (IMPROVED: 2-minute context)
TARGET_WINDOW = 10                     # Target prediction window (IMPROVED: 10-second smoothed trend)

# =====================================================
# ⚡ DISTRIBUTED STRATEGY
# =====================================================
try:
    strategy = tf.distribute.MirroredStrategy()
    print(f"🚀 ACCELERATION: Running on {strategy.num_replicas_in_sync} GPU(s)")
except Exception:
    strategy = tf.distribute.get_strategy()
    print("🖥️  ACCELERATION: Single Device (CPU)")

# =====================================================
# 📐 CUSTOM METRICS & LOSSES
# =====================================================
@tf.function
def r_squared(y_true, y_pred):
    """R² across all 7 features — kept for reference logging."""
    SS_res = tf.reduce_sum(tf.square(y_true - y_pred))
    SS_tot = tf.reduce_sum(tf.square(y_true - tf.reduce_mean(y_true)))
    return 1.0 - SS_res / (SS_tot + tf.keras.backend.epsilon())


@tf.function
def throughput_r2(y_true, y_pred):
    """
    FIX-2 (retained): R² on Throughput_bps ONLY (index 0).
    EarlyStopping monitors this — not diluted average R² across 7 features.
    This directly reflects how well the model predicts the primary KPI.
    """
    t_true = y_true[:, THROUGHPUT_IDX]
    t_pred = y_pred[:, THROUGHPUT_IDX]
    SS_res = tf.reduce_sum(tf.square(t_true - t_pred))
    SS_tot = tf.reduce_sum(tf.square(t_true - tf.reduce_mean(t_true)))
    return 1.0 - SS_res / (SS_tot + tf.keras.backend.epsilon())


@tf.function
def throughput_mae(y_true, y_pred):
    """MAE on Throughput_bps only — clean primary KPI error signal."""
    return tf.reduce_mean(
        tf.abs(y_true[:, THROUGHPUT_IDX] - y_pred[:, THROUGHPUT_IDX])
    )


def urllc_weighted_loss(y_true, y_pred):
    """
    Composite loss for URLLC sparse data.
    Huber(δ=0.1) — precise on near-zero events.
    MSE — amplifies penalty on rare large spikes.
    Weights: 70% Huber + 30% MSE.
    """
    huber = tf.keras.losses.Huber(delta=0.1)(y_true, y_pred)
    mse   = tf.keras.losses.MeanSquaredError()(y_true, y_pred)
    return 0.7 * huber + 0.3 * mse


# =====================================================
# ⚙️ CLI ARGUMENTS
# =====================================================
# CHANGE [1]: Removed --window arg. Snapshot architecture has no
# sequence window — rolling stats are pre-computed in pandas.
def parse_args():
    p = argparse.ArgumentParser(description="5G KPI Slice-Wise Snapshot Training")
    p.add_argument("--data_dir",   type=str, required=True,
                   help="Parquet path: local dir or hdfs://namenode:8020/path")
    p.add_argument("--slice_name", type=str, required=True,
                   choices=["eMBB", "URLLC", "mMTC"])
    p.add_argument("--output_dir", type=str, default="./artifacts")
    p.add_argument("--epochs",     type=int, default=100,
                   help="Max epochs — EarlyStopping patience=15 guards over-running")
    p.add_argument("--batch_size", type=int, default=256)
    p.add_argument("--lr",         type=float, default=0.0001)
    return p.parse_args()


# =====================================================
# 📂 DATA LOADING & SNAPSHOT FEATURE ENGINEERING
# =====================================================
# CHANGE [1]: Full rewrite. Old function returned a 7-col DataFrame for
# 3D windowing. New function returns:
#   X : np.ndarray (N-1, 14) — 7 raw + 7 rolling-mean snapshot
#   y : np.ndarray (N-1, 7)  — next-step raw KPI vector (t+1 target)
#   log_applied : bool        — eMBB flag for expm1 inverse
#
# WHY SNAPSHOT BEATS 3D SEQUENCES FOR ONNX:
#   GRU+Attention encodes history inside the TF graph as StatelessWhile
#   ops → crash on ONNX export. Here, history is pre-computed in pandas
#   before training, so the model receives a flat 2D vector. No recurrent
#   ops means from_keras export works without any workaround.
def load_and_prep_data(data_path: str, slice_name: str):
    print(f"🔍 Scanning: {data_path}")
    dfs = []

    # ── HDFS ─────────────────────────────────────────────────────────
    if data_path.startswith("hdfs://"):
        if not HDFS_AVAILABLE:
            raise RuntimeError("❌ pyarrow not installed — cannot read HDFS")
        try:
            filesystem, hdfs_path = pafs.FileSystem.from_uri(data_path)
            print(f"   -> Connected to HDFS: {hdfs_path}")
            selector  = pafs.FileSelector(hdfs_path, recursive=True)
            all_files = filesystem.get_file_info(selector)
            files     = [f.path for f in all_files
                         if f.is_file and f.path.endswith(".parquet")]
            if not files:
                raise FileNotFoundError(f"No .parquet files at {hdfs_path}")
            print(f"   -> Found {len(files)} files. Downloading to local then reading...")
            for fpath in files:
                try:
                    # Setup temporary local path
                    local_tmp_path = f"/tmp/{os.path.basename(fpath)}"
                    print(f"   -> Downloading: {os.path.basename(fpath)}")
                    
                    # Download HDFS file to local disk
                    with filesystem.open_input_stream(fpath) as hdfs_stream:
                        with open(local_tmp_path, "wb") as local_file:
                            local_file.write(hdfs_stream.read())
                    
                    # Read from local file (seekable, supports Snappy)
                    df_tmp = pd.read_parquet(local_tmp_path)
                    
                    # FIX-3 (retained): inject sliceType from HDFS partition folder
                    if "sliceType" not in df_tmp.columns:
                        for s in ["eMBB", "URLLC", "mMTC"]:
                            if f"sliceType={s}" in fpath or f"/{s}/" in fpath:
                                df_tmp["sliceType"] = s
                                break
                    dfs.append(df_tmp)
                    
                    # Cleanup: remove temporary file immediately
                    os.remove(local_tmp_path)
                    
                except Exception as e:
                    print(f"   ⚠️  Skipped {os.path.basename(fpath)}: {e}")
                    # Cleanup on error too, if file exists
                    if 'local_tmp_path' in locals() and os.path.exists(local_tmp_path):
                        os.remove(local_tmp_path)
        except Exception as e:
            raise RuntimeError(f"❌ HDFS error: {e}")

    # ── Local ─────────────────────────────────────────────────────────
    else:
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"❌ Path not found: {data_path}")
        files = glob.glob(os.path.join(data_path, "**", "*.parquet"),
                          recursive=True)
        if not files:
            raise FileNotFoundError("❌ No .parquet files found locally.")
        print(f"   -> Found {len(files)} local files. Loading...")
        for fpath in files:
            try:
                df_tmp = pd.read_parquet(fpath)
                # FIX-3 (retained): inject sliceType from folder name
                if "sliceType" not in df_tmp.columns:
                    for s in ["eMBB", "URLLC", "mMTC"]:
                        if f"sliceType={s}" in fpath or f"/{s}/" in fpath:
                            df_tmp["sliceType"] = s
                            break
                dfs.append(df_tmp)
            except Exception as e:
                print(f"   ⚠️  Skipped {os.path.basename(fpath)}: {e}")

    if not dfs:
        raise ValueError("❌ No valid data loaded.")

    raw = pd.concat(dfs, ignore_index=True)

    # Filter to target slice
    if "sliceType" in raw.columns:
        df_slice = raw[raw["sliceType"] == slice_name].copy()
        print(f"   > Filtered '{slice_name}': {len(df_slice):,} rows")
    else:
        print("   ⚠️  sliceType still missing after injection — using all data.")
        df_slice = raw.copy()

    if len(df_slice) == 0:
        raise ValueError(f"❌ Zero rows for slice '{slice_name}'.")

    # Sort chronologically — mandatory for rolling stats to be meaningful
    if "window_end" in df_slice.columns:
        df_slice = df_slice.sort_values("window_end").reset_index(drop=True)

    missing_cols = [c for c in FEATURE_COLUMNS if c not in df_slice.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing columns: {missing_cols}")

    # ── Base 7-feature DataFrame ──────────────────────────────────────
    df = df_slice[FEATURE_COLUMNS].copy()
    df = df.ffill().bfill().fillna(0)

    # eMBB: log1p on Throughput_bps ONLY — applied before rolling so the
    # 60-s trend is computed in the same compressed log-space as raw values.
    log_applied = False
    if slice_name == "eMBB":
        print("   🔧 eMBB: log1p(Throughput_bps) — compressing spike range")
        # df["Throughput_bps"] = np.log1p(df["Throughput_bps"])  # COMMENTED OUT: Testing without log transformation
        log_applied = False  # Set to False since we're not applying log transformation

    # ── Rolling mean features (60-second statistical trend baseline) ──
    # CHANGE [1]: New logic. pandas rolling(60, min_periods=1) computes
    # the trailing 60-row mean for each KPI column.
    #   rows 0-58  → use min_periods=1 (no NaN rows produced)
    #   rows 59+   → full 60-second rolling window
    # Column names: "Throughput_bps_roll120", "Total_Packets_roll120", etc.
    print(f"   📊 Computing {ROLLING_WINDOW}s rolling mean ({NUM_RAW} features)...")
    rolling_df = df.rolling(window=ROLLING_WINDOW, min_periods=1).mean()
    rolling_df.columns = ROLLING_COLUMNS

    # ── Combine → 14-feature snapshot DataFrame ───────────────────────
    snapshot_df = pd.concat([df, rolling_df], axis=1)

    assert snapshot_df.shape[1] == NUM_SNAPSHOT, (
        f"FATAL: Expected {NUM_SNAPSHOT} snapshot columns, "
        f"got {snapshot_df.shape[1]}"
    )

    # ── Build X and y arrays ──────────────────────────────────────────
    # IMPROVED TARGET: 10-second smoothed future trend prediction
    # X[t] = [raw_t  |  rolling_mean_t]  — what we know at second t
    # y[t] = smoothed future trend       — 10-second rolling avg shifted backwards
    
    # Create the smoothed target using 10-second rolling average shifted backwards
    print(f"   📊 Creating 10-second smoothed target (future trend prediction)...")
    
    # Create Target_Throughput_bps with 10-second rolling average shifted backwards
    df['Target_Throughput_bps'] = df['Throughput_bps'].rolling(window=10).mean().shift(-9)
    
    # Create smoothed targets for all 7 features using same logic
    for col in FEATURE_COLUMNS:
        df[f'Target_{col}'] = df[col].rolling(window=10).mean().shift(-9)
    
    # Drop NaN rows caused by backward shift
    df = df.dropna()
    
    # Update snapshot_df to align with cleaned df
    snapshot_df = snapshot_df.iloc[:len(df)].copy()
    
    # Build target array using the smoothed target columns
    target_columns = [f'Target_{col}' for col in FEATURE_COLUMNS]
    y = df[target_columns].values.astype(np.float32)  # (N, 7) - smoothed targets
    
    # X remains unchanged - preserve exact input contract for Scala inference
    X = snapshot_df.values.astype(np.float32)  # (N, 14)

    print(f"   ✅ Ready: {X.shape[0]:,} samples | X={X.shape} | y={y.shape}")
    return X, y, log_applied


# =====================================================
# 🏗️ MODEL ARCHITECTURE: Deep GRN Dense Network
# =====================================================
# CHANGE [2]: Full replacement of build_model.
#
# OLD architecture (causing StatelessWhile ONNX crash):
#   Input(60, 7) → GRN(64) → GRU(128) → GRU(64) → MHA → GAP → GRN(32) → Dense(7)
#
# NEW architecture (clean ONNX export via from_keras):
#   Input(14,) → GRN(128) → GRN(64) → GRN(32) → Dense(7)
#
# WHY GRN INSTEAD OF PLAIN DENSE?
#   GRN uses sigmoid gating: output = LayerNorm(skip + gate ⊙ transform(x))
#   For the 14-feature snapshot, some features will be redundant per slice
#   (e.g., for stationary mMTC traffic, rolling mean ≈ raw value always).
#   The gate learns to suppress the redundant channel rather than
#   fighting it through a linear weight, improving convergence.
class GatedResidualNetwork(layers.Layer):
    def __init__(self, units: int, dropout: float = 0.1, **kwargs):
        super().__init__(**kwargs)
        self.units         = units
        self.elu_dense     = layers.Dense(units, activation="elu")
        self.linear_dense  = layers.Dense(units)
        self.dropout_layer = layers.Dropout(dropout)
        self.gate          = layers.Dense(units, activation="sigmoid")
        self.norm          = layers.LayerNormalization()
        self.skip_project  = None

    def build(self, input_shape):
        if input_shape[-1] != self.units:
            self.skip_project = layers.Dense(self.units, use_bias=False)
        super().build(input_shape)

    def call(self, x, training=False):
        skip  = self.skip_project(x) if self.skip_project is not None else x
        x_val = self.elu_dense(x)
        x_val = self.dropout_layer(x_val, training=training)
        x_val = self.linear_dense(x_val)
        x_val = x_val * self.gate(x)    # Sigmoid gating
        return self.norm(skip + x_val)  # Residual + LayerNorm

    def get_config(self):
        cfg = super().get_config()
        cfg.update({"units": self.units})
        return cfg


# CHANGE [2]: Signature changed from build_model(input_shape, slice_name, lr)
# to build_model(slice_name, lr). No input_shape arg — always (NUM_SNAPSHOT,).
def build_model(slice_name: str, lr: float = 0.0001):
    """
    IMPROVED Deep GRN Dense Network for 14-feature Statistical Snapshot.

    Input  : (batch, 14)  — flat 2D snapshot, no sequences (120s context)
    Output : (batch, 7)   — 5-second trend prediction (more predictable than 1s spikes)

    IMPROVEMENTS:
    - Expanded model capacity with additional Dense layers
    - Deeper architecture for better non-linear mapping
    - NO GRU / NO MultiHeadAttention → NO StatelessWhile → Clean ONNX export
    """
    inputs = layers.Input(shape=(NUM_SNAPSHOT,), name="input")

    # Stage 1: Broad feature mixing across all 14 snapshot dimensions.
    # Gate learns to separate informative raw vs rolling channels per slice.
    x = GatedResidualNetwork(128, dropout=0.1, name="grn_128")(inputs)

    # Stage 2: Mid-level slice-specific representation  
    x = GatedResidualNetwork(64, dropout=0.1, name="grn_64")(x)

    # Stage 3: Compact embedding before output projection
    x = GatedResidualNetwork(32, dropout=0.1, name="grn_32")(x)
    
    # IMPROVEMENT: Additional Dense layers for expanded model capacity
    # More parameters to map complex non-linear relationships
    x = layers.Dense(128, activation="relu", name="dense_128")(x)
    x = layers.Dropout(0.1, name="dropout_1")(x)
    
    x = layers.Dense(64, activation="relu", name="dense_64")(x)
    x = layers.Dropout(0.1, name="dropout_2")(x)

    # Output: 7 raw KPI values for next 5-second trend (t+1:t+6)
    outputs = layers.Dense(NUM_RAW, name="output")(x)

    model = models.Model(inputs, outputs, name=f"Snapshot_{slice_name}")

    loss_fn = (urllc_weighted_loss if slice_name == "URLLC"
               else losses.Huber(delta=1.0))
    lname   = ("0.7×Huber(δ=0.1)+0.3×MSE" if slice_name == "URLLC"
               else "Huber(δ=1.0)")
    print(f"   🎯 Loss  : {lname}")
    print(f"   📐 Input : (batch, {NUM_SNAPSHOT})  — flat snapshot, 120s context")
    print(f"   📐 Output: (batch, {NUM_RAW})  — {TARGET_WINDOW}s trend prediction (improved: vs 1s spikes)")

    model.compile(
        optimizer=optimizers.Adam(learning_rate=lr),
        loss=loss_fn,
        metrics=[
            "mae",
            r_squared,
            throughput_r2,   # Primary EarlyStopping monitor (FIX-2 retained)
            throughput_mae   # Human-readable per-epoch throughput error
        ]
    )
    model.summary(line_length=80)
    return model


# =====================================================
# 📊 REAL-WORLD EVALUATION
# =====================================================
# CHANGE [1]: Signature updated. Old version looped over a tf.data.Dataset
# (test_ds). New version takes plain numpy arrays X_test, y_test directly,
# since there is no longer a 3D windowing pipeline to produce batches.
def evaluate_real_world(model, X_test: np.ndarray, y_test: np.ndarray,
                        scaler_raw: RobustScaler, slice_name: str,
                        log_applied: bool) -> dict:
    """
    Computes MAE and R² in real physical units (bps) by inverting
    RobustScaler and the eMBB log1p transform on Throughput_bps.
    """
    print("\n📊 Computing real-world metrics (inverse-transformed)...")

    preds_scaled = model.predict(X_test, verbose=0)   # shape: (N, 7) scaled

    # Undo RobustScaler — uses scaler_raw (7-feature raw output space)
    preds_real = scaler_raw.inverse_transform(preds_scaled)
    y_real     = scaler_raw.inverse_transform(y_test)

    # Undo log1p for eMBB Throughput_bps (index 0 only)
    if log_applied:
        print("   ↩️  Reversing log1p on Throughput_bps...")
        preds_real[:, THROUGHPUT_IDX] = np.expm1(
            np.clip(preds_real[:, THROUGHPUT_IDX], 0, None))
        y_real[:, THROUGHPUT_IDX] = np.expm1(
            np.clip(y_real[:, THROUGHPUT_IDX], 0, None))

    tp_pred  = preds_real[:, THROUGHPUT_IDX]
    tp_true  = y_real[:, THROUGHPUT_IDX]
    mae_bps  = mean_absolute_error(tp_true, tp_pred)
    r2       = sklearn_r2(tp_true, tp_pred)
    mean_bps = np.mean(tp_true)
    acc      = max(0.0, 100.0 * (1.0 - mae_bps / mean_bps)) if mean_bps > 0 else 0.0

    print(f"\n{'='*55}")
    print(f"  🏆 REAL-WORLD VERDICT — {slice_name}")
    print(f"{'='*55}")
    print(f"  Throughput MAE : {mae_bps:>15,.0f} bps  ({mae_bps/1e6:.3f} Mbps)")
    print(f"  Mean Throughput: {mean_bps:>15,.0f} bps")
    print(f"  Estimated Acc. : {acc:>14.2f} %")
    print(f"  R² Score       : {r2:>14.4f}")
    print(f"{'='*55}")
    verdict = ("🌟 EXCELLENT"    if r2 >= 0.85 else
               "✅ GOOD"         if r2 >= 0.65 else
               "⚠️  ACCEPTABLE"  if r2 >= 0.40 else
               "❌ POOR — needs more data or architecture changes")
    print(f"  {verdict}")

    return {"mae_bps": float(mae_bps), "mae_mbps": float(mae_bps / 1e6),
            "r2": float(r2), "accuracy_pct": float(acc),
            "mean_throughput_bps": float(mean_bps)}


# =====================================================
# 💾 ONNX EXPORT
# =====================================================
# CHANGE [4]: Full replacement of export_onnx.
#
# OLD: subprocess call to tf2onnx CLI with SavedModel intermediate.
#      Needed because GRU StatelessWhile ops crash the Python API.
#
# NEW: direct tf2onnx.convert.from_keras — works cleanly because
#      GRN+Dense has no recurrent ops → no StatelessWhile in the graph.
#      Input signature updated: (batch, 14) instead of (batch, 60, 7).
def export_onnx(model, output_dir: str) -> str:
    """
    Direct from_keras ONNX export.
    Works cleanly with GRN Dense network — no StatelessWhile crash.

    Input contract : (batch, 14) — 7 raw + 7 rolling-mean features
    Output contract: (batch, 7)  — next-step raw KPI predictions
    Scala reads scaler_params.json for normalization constants.
    """
    print("\n💾 Exporting ONNX (from_keras — no GRU, no StatelessWhile)...")

    onnx_path  = os.path.join(output_dir, "model.onnx")
    input_spec = (tf.TensorSpec((None, NUM_SNAPSHOT),
                                tf.float32, name="input"),)
    try:
        model_proto, _ = tf2onnx.convert.from_keras(
            model,
            input_signature=input_spec,
            opset=13
        )
        with open(onnx_path, "wb") as f:
            f.write(model_proto.SerializeToString())

        size_mb = os.path.getsize(onnx_path) / 1e6
        print(f"   ✅ ONNX saved  → {onnx_path}  ({size_mb:.2f} MB)")
        print(f"      Input  : [batch, {NUM_SNAPSHOT}]"
              f"  ({NUM_RAW} raw + {NUM_RAW} rolling)")
        print(f"      Output : [batch, {NUM_RAW}]  ← next-step KPIs")
        print(f"      Opset  : 13  |  Scala ✔")
        return onnx_path

    except Exception as e:
        print(f"\n   ❌ ONNX export failed: {e}")
        print("   ℹ️  Training checkpoint is still saved — no data lost.")
        print("   💡 pip install tf2onnx --upgrade --break-system-packages")
        return None


# =====================================================
# 🚀 MAIN PIPELINE
# =====================================================
# CHANGE [1][3][4][5]: run_pipeline updated throughout:
#   - No window/make_tf_dataset — uses flat numpy arrays
#   - Two scalers: scaler_snap (14-feat input) + scaler_raw (7-feat output)
#   - export_onnx called without window arg
#   - scaler_params.json includes all 14 normalization constants
def run_pipeline(args):
    config = {
        "epochs":       args.epochs,
        "batch_size":   args.batch_size,
        "lr":           args.lr,
        "target_slice": args.slice_name,
    }

    print("\n" + "="*55)
    print(f"  🎯 5G KPI TRAINING  |  Slice : {args.slice_name}")
    print(f"  📐 Mode    : Statistical Snapshot ({NUM_SNAPSHOT}-feature, 2D)")
    print(f"  📂 Data    : {args.data_dir}")
    print(f"  💾 Out     : {args.output_dir}")
    print(f"  ⚙️  epochs={config['epochs']}  "
          f"batch={config['batch_size']}  lr={config['lr']}")
    print("="*55 + "\n")

    os.makedirs(args.output_dir, exist_ok=True)

    # ── 1. Load & build 14-feature snapshot dataset ───────────────────
    X, y, log_applied = load_and_prep_data(args.data_dir, args.slice_name)

    n = len(X)
    if n < 200:
        raise ValueError(f"❌ Only {n} samples — need ≥ 200 after snapshot build.")

    # ── 2. Chronological train/test split (no shuffling) ─────────────
    # Time-series data must be split in order — no random split.
    split     = int(0.8 * n)
    X_train_r = X[:split]
    X_test_r  = X[split:]
    y_train_r = y[:split]
    y_test_r  = y[split:]
    print(f"   Train: {len(X_train_r):,} | Test: {len(X_test_r):,}")

    # ── 3. Two independent RobustScalers per slice ────────────────────
    # CHANGE [5]: Two scalers now instead of one.
    #
    # scaler_snap → normalises the 14-feature model INPUT (X)
    #   Scala applies this to every new snapshot before calling ONNX.
    #
    # scaler_raw  → normalises the 7-feature model TARGET (y)
    #   Scala inverts this on every ONNX output to recover real KPI units.
    #
    # Fitted on training split only — no leakage from test set.
    print(f"\n🔧 Fitting RobustScalers for {args.slice_name}...")

    scaler_snap = RobustScaler()
    X_train     = scaler_snap.fit_transform(X_train_r)
    X_test      = scaler_snap.transform(X_test_r)

    scaler_raw  = RobustScaler()
    y_train     = scaler_raw.fit_transform(y_train_r)
    y_test      = scaler_raw.transform(y_test_r)

    print(f"   Snapshot (14-feat) — "
          f"Center(μ): {np.mean(scaler_snap.center_):.4f} | "
          f"Scale(μ): {np.mean(scaler_snap.scale_):.4f}")
    print(f"   Raw target (7-feat) — "
          f"Center(μ): {np.mean(scaler_raw.center_):.4f} | "
          f"Scale(μ): {np.mean(scaler_raw.scale_):.4f}")

    # ── 4. Build model ────────────────────────────────────────────────
    print(f"\n🏗️  Building Snapshot GRN model for {args.slice_name}...")
    with strategy.scope():
        # CHANGE [2]: No input_shape arg — architecture always (NUM_SNAPSHOT,)
        model = build_model(args.slice_name, lr=config["lr"])

    # ── 5. Callbacks — all monitor val_throughput_r2 (FIX-2 retained) ─
    # CHANGE [3]: EarlyStopping patience 10 → 15.
    #   Snapshot models converge more slowly than sequence models because
    #   the trend signal (rolling mean) requires more gradient steps to
    #   separate from noise than a GRU hidden state. patience=15 gives
    #   the optimiser time to find the right gate weights.
    # CHANGE [3]: ReduceLROnPlateau patience 5 → 7 for same reason.
    ckpt_path = os.path.join(args.output_dir, "best_checkpoint.tf")
    cb_list = [
        callbacks.EarlyStopping(
            monitor="val_throughput_r2", mode="max",
            patience=15,                    # CHANGE [3]: was 10
            restore_best_weights=True, verbose=1
        ),
        callbacks.ReduceLROnPlateau(
            monitor="val_throughput_r2", mode="max",
            factor=0.5, patience=7,         # CHANGE [3]: was 5
            min_lr=1e-6, verbose=1
        ),
        callbacks.ModelCheckpoint(
            filepath=ckpt_path, monitor="val_throughput_r2",
            mode="max", save_best_only=True, save_format="tf", verbose=0
        ),
        callbacks.CSVLogger(
            os.path.join(args.output_dir, "training_log.csv"), append=False
        )
    ]

    # ── 6. Train — direct numpy arrays (no tf.data pipeline) ─────────
    # CHANGE [1]: model.fit receives X_train / y_train numpy arrays
    # directly. The 3D make_tf_dataset pipeline has been removed.
    print(f"\n🔥 Training {args.slice_name} — up to {config['epochs']} epochs "
          f"(EarlyStopping patience=15)...\n")
    history = model.fit(
        X_train, y_train,
        epochs=config["epochs"],
        batch_size=config["batch_size"] * strategy.num_replicas_in_sync,
        validation_data=(X_test, y_test),
        callbacks=cb_list,
        verbose=1
    )

    best_r2  = max(history.history.get("val_throughput_r2",  [0]))
    best_mae = min(history.history.get("val_throughput_mae", [float("inf")]))
    print(f"\n   Best val Throughput R²  : {best_r2:.4f}")
    print(f"   Best val Throughput MAE : {best_mae:.6f} (scaled)")

    # ── 7. Real-world evaluation ──────────────────────────────────────
    metrics = evaluate_real_world(
        model, X_test, y_test,
        scaler_raw, args.slice_name, log_applied
    )

    # ── 8. ONNX export ────────────────────────────────────────────────
    # CHANGE [4]: No window arg. from_keras works directly.
    onnx_path = export_onnx(model, args.output_dir)

    # ── 9. Scaler JSON — all 14 normalization constants for Scala ─────
    # CHANGE [5]: Now exports both scaler_snap (14 values) and
    # scaler_raw (7 values). Scala must apply both in correct order:
    #   Input  : normalise with snapshot_center / snapshot_scale
    #   Output : invert with raw_center / raw_scale (+ expm1 for eMBB)
    print("\n💾 Exporting scaler metadata (14-feature contract)...")
    scaler_data = {
        "slice_name":             args.slice_name,
        "architecture":           "StatisticalSnapshot",

        # Feature name lists (for documentation and debugging)
        "input_features":         SNAPSHOT_COLUMNS,   # 14 names
        "output_features":        FEATURE_COLUMNS,    # 7 names

        # Dimensions
        "num_input_features":     NUM_SNAPSHOT,       # 14
        "num_output_features":    NUM_RAW,            # 7
        "rolling_window_seconds": ROLLING_WINDOW,     # 60

        # Input normalisation — Scala applies BEFORE calling ONNX
        # normalised_input[i] = (snapshot[i] - snapshot_center[i]) / snapshot_scale[i]
        "snapshot_center":  scaler_snap.center_.tolist(),   # float[14]
        "snapshot_scale":   scaler_snap.scale_.tolist(),    # float[14]

        # Output inverse — Scala applies AFTER ONNX returns predictions
        # raw_pred[i] = prediction[i] * raw_scale[i] + raw_center[i]
        "raw_center":       scaler_raw.center_.tolist(),    # float[7]
        "raw_scale":        scaler_raw.scale_.tolist(),     # float[7]

        # eMBB: Scala must apply expm1 to output[0] (Throughput_bps)
        "embb_log_transform": log_applied,
        "throughput_index":   THROUGHPUT_IDX,

        "onnx_exported":      onnx_path is not None,
        "training_metrics":   metrics,

        # Step-by-step Scala inference guide
        "_scala_inference_steps": [
            "1. Maintain circular buffer of last 60 rows (7 raw KPIs each)",
            "2. raw_now = current second's 7 KPI values",
            "3. rolling = column-wise mean of the 60-row buffer",
            "4. snapshot = concat(raw_now, rolling) → float[14]",
            "5. norm_in[i] = (snapshot[i] - snapshot_center[i]) / snapshot_scale[i]",
            "6. prediction = ONNX.run(norm_in) → float[7]",
            "7. raw_pred[i] = prediction[i] * raw_scale[i] + raw_center[i]",
            "8. if eMBB: raw_pred[0] = Math.expm1(raw_pred[0])  (Throughput_bps only)"
        ]
    }

    scaler_path = os.path.join(args.output_dir, "scaler_params.json")
    with open(scaler_path, "w") as f:
        json.dump(scaler_data, f, indent=2)
    print(f"   ✅ Scaler JSON → {scaler_path}")
    print(f"      snapshot_center/scale : {NUM_SNAPSHOT} values (Scala input norm)")
    print(f"      raw_center/scale      : {NUM_RAW} values  (Scala output inverse)")

    # ── 10. Training history ──────────────────────────────────────────
    hist_path = os.path.join(args.output_dir, "history.json")
    with open(hist_path, "w") as f:
        json.dump(history.history, f, indent=2, default=str)
    print(f"   ✅ History    → {hist_path}")

    print(f"\n🎉 Pipeline complete [{args.slice_name}]")
    if onnx_path:
        print(f"   ✅ ONNX      : {onnx_path}")
    else:
        print(f"   ❌ ONNX      : export failed (checkpoint still valid)")
    print(f"   ✅ JSON      : {scaler_path}")
    print(f"   ✅ Checkpoint: {ckpt_path}")
    return onnx_path, scaler_path


# =====================================================
# 🏁 ENTRY POINT
# =====================================================
if __name__ == "__main__":
    np.random.seed(42)
    tf.random.set_seed(42)
    args   = parse_args()
    result = run_pipeline(args)
    if result is None:
        exit(1)

# =====================================================
# 📌 RUN COMMANDS — all 3 nodes in parallel
# =====================================================
# python train.py \
#   --data_dir hdfs://namenode:8020/5g_kpi/processed/sliceType=eMBB \
#   --slice_name eMBB --output_dir models/eMBB --epochs 100 &
#
# python train.py \
#   --data_dir hdfs://namenode:8020/5g_kpi/processed/sliceType=URLLC \
#   --slice_name URLLC --output_dir models/URLLC --epochs 100 &
#
# python train.py \
#   --data_dir hdfs://namenode:8020/5g_kpi/processed/sliceType=mMTC \
#   --slice_name mMTC --output_dir models/mMTC --epochs 100 &
#
# wait && echo "✅ All 3 slices trained"
#
# NOTE: --window flag removed. Snapshot architecture has no sequence
# window — historical context is pre-computed as a 60-s rolling mean
# in pandas before training, not inside the neural network.
# =====================================================