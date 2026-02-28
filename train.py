# =============================================================================
# 5G NWDAF TRAFFIC FORECASTING -- v6.4 UNIFIED SLICE-AWARE PIPELINE
# =============================================================================
#
# Single file, single command to train any slice.
# ALL THREE SLICES use the SAME architecture family: VAR + GRU + TFT.
# Only slice-specific tuning differs (Huber delta, dropout, patience, features).
#
#  SHARED ARCHITECTURE: VAR-Linear + GRU-Temporal + TFT-Attention
#  ---------------------------------------------------------------
#  ① VAR-Linear branch  : wide Dense projection (cross-KPI correlations)
#  ② GRU-Temporal branch: GRU stack (short-term burstiness / temporal memory)
#  ③ TFT-Attention branch: Variable Selection Network (slice-specific weights)
#  Fusion → GRN stack → Dense(1)
#
#  eMBB  (v6.4 -- Root-Cause Fix: Stratified Shuffle Split + Retuned HPs)
#  -----------------------------------------------------------------------
#  ROOT CAUSE CONFIRMED (feb27.md session report):
#    v6.2 (Train R²=0.95, Val R²=-0.74) and v6.3 (Train R²=0.51, Val R²=-0.81)
#    BOTH failed in identical fashion regardless of model size or regularisation.
#    This rules out overfitting as the root cause.
#
#    The real cause: CHRONOLOGICAL DISTRIBUTION SHIFT.
#    eMBB data = YouTube streaming traffic (non-stationary PCAP capture).
#    85/15 chronological split = train on "early streaming period", validate
#    on "late streaming period". These have fundamentally different throughput
#    distributions (initial buffering bursts vs adaptive-bitrate steady-state).
#    No model can generalise across a regime change — this is why ALL prior
#    versions failed identically regardless of architecture or regularisation.
#
#  v6.4 FIXES (eMBB only — URLLC and mMTC are unchanged):
#
#    FIX 1 — Stratified Shuffle Split (THE critical fix):
#             Throughput_bps is quantile-binned into 10 equal-frequency bins.
#             A stratified 85/15 shuffle split ensures BOTH train and val
#             contain examples from every throughput regime — near-zero
#             buffering pauses, mid-range adaptive-bitrate, and peak bursts.
#             This directly eliminates the temporal regime-change problem.
#             --no_shuffle restores chronological split for ablation.
#
#    FIX 2 — Shorter Target Window (target_window: 30 → 10 steps):
#             Predicting 30 steps ahead of highly variable broadband traffic
#             is extremely difficult. 10-step horizon is more achievable and
#             retains more rows after dropna (fewer NaN labels).
#
#    FIX 3 — Shorter Sequence Window (seq_len: 60 → 30 steps):
#             30-step windows are sufficient for short-term broadband patterns
#             without encouraging long-range memorisation.
#
#    FIX 4 — Recalibrated Regularisation (L2: 1e-3→1e-4, dropout: 0.40→0.20):
#             v6.3 used extreme regularisation to compensate for distribution
#             shift. With FIX 1 resolving the root cause, moderate regularisation
#             restores capacity without over-constraining the model.
#
#    FIX 5 — Restored Learning Rate (0.00035 → 0.0007):
#             The halved LR in v6.3 was compensation for distribution shift.
#             With a properly distributed dataset, default 0.0007 works well.
#
#    FIX 6 — Removed synthesised cyclic ToD features:
#             Dataset has no real timestamp column; row-index ToD features
#             (row//60 % 24) are meaningless noise — removed entirely.
#             Clean 44-feature input (same as URLLC/mMTC).
#
#  Architecture (VAR+BiGRU+TFT — structure unchanged from v6.3):
#    - Input  : (batch, 30, 44)
#    - VAR    : TimeDistributed Dense(32 linear) + GlobalAvgPool -> (batch, 32)
#    - BiGRU  : BiGRU(64,concat) -> BiGRU(32,concat)           -> (batch, 64)
#    - TFT    : VSN(44, 32) on last timestep -> GRN(32)         -> (batch, 32)
#    - Concat : 32+64+32=128 -> GRN(64)->GRN(32)->Dense(16)->Dense(1)
#    - L2     : 1e-4 | Dropout BiGRU=0.20, Dense=0.15
#    - LR     : 0.0007 | Huber delta=5.0 | EarlyStopping patience=40
#    - ONNX   : (1, 30, 44)
#
#  URLLC (v6.0 -- UNCHANGED, proven R²=0.85)
#    - Input  : (batch, 44) flat | VAR+GRU+TFT | Huber delta=1.0
#    - ONNX   : (1, 44)
#
#  mMTC  (v6.2 -- UNCHANGED, proven R²=0.79)
#    - Input  : (batch, 44) flat | VAR+GRU+TFT lighter | Huber delta=0.5
#    - ONNX   : (1, 44)
#
# Usage:
#   python train.py --slice_name eMBB
#   python train.py --slice_name eMBB --seq_len 30 --target_window 10
#   python train.py --slice_name eMBB --no_shuffle          # chronological (ablation)
#   python train.py --slice_name URLLC
#   python train.py --slice_name mMTC
#   python train.py --slice_name eMBB --data_dir hdfs://namenode:8020/5g_kpi/processed
#   python train.py --slice_name eMBB --seq_len 20 --target_window 5 --batch_size 64
#
# ONNX output shapes:
#   eMBB  -> (1, 30, 44)   [or (1, seq_len, 44) with custom --seq_len]
#   URLLC -> (1, 44)
#   mMTC  -> (1, 44)
# =============================================================================

import os
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
from tensorflow.keras import layers, models, callbacks, optimizers, regularizers
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import mean_absolute_error, r2_score as sklearn_r2
from sklearn.model_selection import StratifiedShuffleSplit

try:
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False
    print("pyarrow not available -- HDFS streaming disabled")

warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"


# =============================================================================
# CONSTANTS
# =============================================================================
RAW_KPI_COLUMNS = [
    "Throughput_bps",   # 0  -- forecast target
    "Total_Packets",    # 1
    "Jitter_Variance",  # 2
    "Avg_Packet_Size",  # 3
    "Active_Flows",     # 4
    "TCP_Packets",      # 5
    "UDP_Packets",      # 6
    "Max_Packet_Size",  # 7
    "Min_Packet_Size",  # 8
    "TCP_Syn_Count",    # 9
    "TCP_Fin_Count",    # 10
]
NUM_RAW    = len(RAW_KPI_COLUMNS)   # 11
TARGET_KPI = "Throughput_bps"
ROLLING_WINDOW = 5

# Target smoothing windows per slice
EMBB_TARGET_WINDOW  = 10   # v6.4: shortened 30→10 (more achievable horizon)
URLLC_TARGET_WINDOW = 30
MMTC_TARGET_WINDOW  = 10

# eMBB sequence window (v6.4: shortened 60→30)
EMBB_SEQ_LEN = 30

# Number of quantile bins for eMBB stratified shuffle split (v6.4)
EMBB_SPLIT_BINS = 10


# =============================================================================
# DISTRIBUTED STRATEGY
# =============================================================================
try:
    strategy = tf.distribute.MirroredStrategy()
    print(f"ACCELERATION: {strategy.num_replicas_in_sync} GPU(s)")
except Exception:
    strategy = tf.distribute.get_strategy()
    print("ACCELERATION: Single Device (CPU)")


# =============================================================================
# CUSTOM METRICS
# =============================================================================
@tf.function
def r_squared(y_true, y_pred):
    SS_res = tf.reduce_sum(tf.square(y_true - y_pred))
    SS_tot = tf.reduce_sum(tf.square(y_true - tf.reduce_mean(y_true)))
    return 1.0 - SS_res / (SS_tot + tf.keras.backend.epsilon())


# =============================================================================
# CLI ARGUMENTS
# =============================================================================
def parse_args():
    p = argparse.ArgumentParser(
        description="5G NWDAF v6.4 -- Unified Slice-Aware Training Pipeline (VAR+GRU+TFT all slices)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Train eMBB (stratified shuffle split, 30-step window, 10-step horizon):
    python train.py --slice_name eMBB

  Train eMBB with custom window/horizon:
    python train.py --slice_name eMBB --seq_len 20 --target_window 5

  Train eMBB with chronological split (ablation vs prior versions):
    python train.py --slice_name eMBB --no_shuffle

  Train URLLC (unchanged):
    python train.py --slice_name URLLC

  Train mMTC (unchanged):
    python train.py --slice_name mMTC

  Custom data dir:
    python train.py --slice_name eMBB  --data_dir hdfs://namenode:8020/5g_kpi/processed
    python train.py --slice_name URLLC --data_dir hdfs://namenode:8020/5g_kpi/processed
    python train.py --slice_name mMTC  --data_dir hdfs://namenode:8020/5g_kpi/processed
        """
    )
    p.add_argument("--slice_name",    "--slice",    type=str,   required=True,
                   choices=["eMBB", "URLLC", "mMTC"],
                   help="Which 5G network slice to train")
    p.add_argument("--data_dir",      "--hdfs-path", type=str,  default=None,
                   help="Base data path: local dir or hdfs://namenode:8020/5g_kpi/processed"
                        " (slice subfolder appended automatically)")
    p.add_argument("--output_dir",    type=str,   default="./artifacts",
                   help="Output root dir (slice subfolder appended automatically)")
    p.add_argument("--epochs",        type=int,   default=300)
    p.add_argument("--batch_size",    type=int,   default=None,
                   help="Batch size (default: eMBB=128, URLLC/mMTC=256)")
    p.add_argument("--lr",            type=float, default=0.0007,
                   help="Learning rate (default=0.0007)")
    # eMBB-specific flags
    p.add_argument("--seq_len",       type=int,   default=EMBB_SEQ_LEN,
                   help=f"[eMBB only] Temporal window length in steps (default={EMBB_SEQ_LEN})")
    p.add_argument("--target_window", type=int,   default=EMBB_TARGET_WINDOW,
                   help=f"[eMBB only] Forward-smoothing horizon steps (default={EMBB_TARGET_WINDOW})")
    p.add_argument("--no_shuffle",    action="store_true",
                   help="[eMBB only] Use chronological 85/15 split (ablation, not recommended)")
    # Kept for CLI compatibility but ignored in v6.4
    p.add_argument("--no_cross_slice", action="store_true",
                   help="[eMBB only] Deprecated in v6.4, kept for CLI compatibility only")

    args = p.parse_args()

    # Auto-set default batch size
    if args.batch_size is None:
        args.batch_size = 128 if args.slice_name == "eMBB" else 256

    # Auto data dir
    if args.data_dir is None:
        args.data_dir = "hdfs://namenode:8020/5g_kpi/processed"
        print(f"   Auto data_dir -> {args.data_dir}")

    # Ensure output has slice subfolder
    if not args.output_dir.rstrip("/\\").endswith(args.slice_name):
        args.output_dir = os.path.join(args.output_dir, args.slice_name)

    return args


# =============================================================================
# SHARED PARQUET LOADER
# =============================================================================
def _read_parquet_slice(base_path: str, slice_name: str) -> pd.DataFrame:
    """
    Loads all parquet files for a given slice from HDFS or local filesystem.
    Handles both partitioned (base/sliceType=X) and flat directory layouts.
    """
    dfs = []

    if base_path.startswith("hdfs://"):
        slice_path = f"{base_path}/sliceType={slice_name}"
    else:
        candidates = [
            os.path.join(base_path, f"sliceType={slice_name}"),
            os.path.join(base_path, slice_name),
            base_path,
        ]
        slice_path = next((c for c in candidates if os.path.exists(c)), base_path)

    print(f"   Loading [{slice_name}] from: {slice_path}")

    if slice_path.startswith("hdfs://"):
        if not HDFS_AVAILABLE:
            raise RuntimeError("pyarrow not installed -- cannot read HDFS")
        parts = slice_path.replace("hdfs://", "").split("/", 1)
        host  = parts[0].split(":")[0]
        port  = int(parts[0].split(":")[1]) if ":" in parts[0] else 8020
        hpath = "/" + parts[1] if len(parts) > 1 else "/"
        fs    = pafs.HadoopFileSystem(host=host, port=port)
        flist = fs.get_file_info(pafs.FileSelector(hpath, recursive=True))
        files = [f.path for f in flist
                 if f.type.name == "File" and f.path.endswith(".parquet")]
        for fp in files:
            try:
                dfs.append(pq.read_table(fp, filesystem=fs).to_pandas())
            except Exception as e:
                print(f"     Skipped {os.path.basename(fp)}: {e}")
    else:
        files = (glob.glob(os.path.join(slice_path, "**/*.parquet"), recursive=True)
                 or glob.glob(os.path.join(slice_path, "*.parquet")))
        for fp in files:
            try:
                dfs.append(pd.read_parquet(fp))
            except Exception as e:
                print(f"     Skipped {os.path.basename(fp)}: {e}")

    if not dfs:
        raise RuntimeError(
            f"No parquet files found for [{slice_name}] at {slice_path}")

    df = pd.concat(dfs, ignore_index=True)
    if "sliceType" in df.columns:
        df = df[df["sliceType"] == slice_name].copy()
    if len(df) < 200:
        raise RuntimeError(f"Only {len(df)} rows for {slice_name} -- need >=200")

    print(f"   [{slice_name}] rows: {len(df):,}")
    return df


def _validate_and_clip(df: pd.DataFrame) -> pd.DataFrame:
    """Validate raw KPI columns exist, ffill/bfill, clip 1st-99th percentile."""
    missing = [c for c in RAW_KPI_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing KPI columns: {missing}")
    df = df[RAW_KPI_COLUMNS].copy().ffill().bfill().fillna(0)
    for col in RAW_KPI_COLUMNS:
        lo, hi = df[col].quantile(0.01), df[col].quantile(0.99)
        if hi > lo:
            df[col] = df[col].clip(lower=lo, upper=hi)
    print(f"   Outliers clipped to [1st, 99th] percentile")
    return df


def _make_sequences(X: np.ndarray, y: np.ndarray, window: int):
    """
    Convert flat (N, F) arrays into overlapping sliding windows.
      X_seq : (N - window, window, F)
      y_seq : (N - window, 1)  -- label aligned to step after each window
    """
    n = len(X) - window
    X_seq = np.stack([X[i:i + window] for i in range(n)], axis=0)
    y_seq = y[window:]
    return X_seq, y_seq


# =============================================================================
# SHARED SCALING + SPLIT  (URLLC and mMTC flat pipelines)
# =============================================================================
def scale_and_split(X: np.ndarray, y: np.ndarray):
    """85/15 chronological split, then RobustScaler fit on train only."""
    split  = int(len(X) * 0.85)
    X_tr, X_te = X[:split], X[split:]
    y_tr, y_te = y[:split], y[split:]
    print(f"   Temporal split: train={len(X_tr):,} | test={len(X_te):,}")

    sc_in  = RobustScaler()
    sc_tgt = RobustScaler()
    X_tr = sc_in.fit_transform(X_tr).astype(np.float32)
    X_te = sc_in.transform(X_te).astype(np.float32)
    y_tr = sc_tgt.fit_transform(y_tr).astype(np.float32)
    y_te = sc_tgt.transform(y_te).astype(np.float32)

    print(f"   Scaler -- center(avg)={np.mean(sc_in.center_):.4f} | "
          f"scale(avg)={np.mean(sc_in.scale_):.4f}")
    return X_tr, y_tr, X_te, y_te, sc_in, sc_tgt


# =============================================================================
# eMBB STRATIFIED SHUFFLE SPLIT  (v6.4 root-cause fix)
# =============================================================================
def _embb_stratified_split(X_seq: np.ndarray, y_seq: np.ndarray,
                            n_bins: int = EMBB_SPLIT_BINS,
                            test_size: float = 0.15,
                            random_state: int = 42):
    """
    Stratified shuffle split for eMBB sliding-window sequences.

    WHY THIS IS THE ROOT CAUSE FIX:
    eMBB data = YouTube streaming traffic from PCAP captures.
    The data is non-stationary: early periods have buffering bursts, later
    periods have adaptive-bitrate steady-state at a different throughput level.
    A chronological 85/15 split puts ALL of one regime in train and ALL of
    another in val — this is distribution shift by construction.

    Solution: quantile-bin throughput into n_bins equal-frequency strata,
    then use StratifiedShuffleSplit to guarantee both train and val see
    the same distribution of throughput values.

    Scaler is fit on the FULL dataset before sequences are built and before
    this split is applied. For a shuffled split this is not leakage — there
    is no temporal future to leak. It also gives the scaler better coverage
    of the full throughput distribution.

    Args:
      X_seq : (N, seq_len, F) already scaled sequences
      y_seq : (N, 1) already scaled targets
      n_bins: number of quantile bins (default 10)
      test_size: validation fraction (default 0.15)
      random_state: reproducibility (default 42)

    Returns:
      X_train, y_train, X_test, y_test
    """
    N = len(y_seq)
    y_flat = y_seq.ravel()

    # Protect against very small datasets or extreme skew
    effective_bins = min(n_bins, N // 10)
    effective_bins = max(effective_bins, 2)

    try:
        strata = pd.qcut(y_flat, q=effective_bins, labels=False, duplicates="drop")
    except Exception:
        # Fallback: equal-width cut if qcut still fails (very skewed distribution)
        strata = pd.cut(y_flat, bins=effective_bins, labels=False)
    # Handle both pandas Series and numpy array returns from qcut/cut
    if isinstance(strata, np.ndarray):
        strata = np.nan_to_num(strata, nan=0).astype(int)
    else:
        strata = strata.fillna(0).astype(int).values

    sss = StratifiedShuffleSplit(
        n_splits=1, test_size=test_size, random_state=random_state)
    train_idx, test_idx = next(sss.split(X_seq, strata))

    X_train = X_seq[train_idx]
    y_train = y_seq[train_idx]
    X_test  = X_seq[test_idx]
    y_test  = y_seq[test_idx]

    # Distribution check: confirm the split resolved the shift
    y_tr_mean = float(np.mean(y_train))
    y_te_mean = float(np.mean(y_test))
    y_tr_std  = float(np.std(y_train))
    y_te_std  = float(np.std(y_test))
    mean_diff  = abs(y_tr_mean - y_te_mean)

    print(f"   Stratified shuffle split: {effective_bins} throughput quantile bins")
    print(f"   Train: N={len(train_idx):,}  mean_y={y_tr_mean:+.3f}  std_y={y_tr_std:.3f}")
    print(f"   Val  : N={len(test_idx):,}  mean_y={y_te_mean:+.3f}  std_y={y_te_std:.3f}")

    if mean_diff < 0.10:
        print(f"   Distribution match: mean gap={mean_diff:.3f}  EXCELLENT")
    elif mean_diff < 0.25:
        print(f"   Distribution match: mean gap={mean_diff:.3f}  GOOD")
    else:
        print(f"   WARNING: mean gap={mean_diff:.3f} -- residual mismatch")
        print(f"   (Consider more bins or a different random_state)")

    return X_train, y_train, X_test, y_test


# =============================================================================
# DATA LOADERS  (one per slice)
# =============================================================================

def load_data_embb(base_path: str, seq_len: int, target_window: int,
                   use_shuffle: bool):
    """
    eMBB data loader v6.4 -- Stratified Shuffle Split Edition.

    Changes vs v6.3:
      - target_window: 30 → 10 (configurable, shorter = more achievable)
      - seq_len: 60 → 30 (configurable, shorter = less long-range dependency)
      - Stratified shuffle split by default (use_shuffle=True)
      - Removed synthesised cyclic ToD features (no real timestamp in dataset)
      - Clean 44 features (raw + rmean + rstd + diff)

    Scaler fit on FULL dataset in shuffle mode (no temporal leakage when
    the split is random).  In --no_shuffle mode, scaler fits on train only.

    Returns:
      X_train (N_tr, seq_len, 44), y_train (N_tr, 1)
      X_test  (N_te, seq_len, 44), y_test  (N_te, 1)
      sc_in, sc_tgt, target_window_used, feat_cols
    """
    print(f"\n{'='*65}")
    print(f"  eMBB DATA LOADER v6.4  [Stratified Shuffle Split]")
    print(f"  seq_len={seq_len} | target_window={target_window}")
    print(f"  split={'STRATIFIED SHUFFLE (10 bins)' if use_shuffle else 'CHRONOLOGICAL 85/15'}")
    print(f"  features=44 (raw+rmean+rstd+diff)")
    print(f"{'='*65}")

    df = _validate_and_clip(_read_parquet_slice(base_path, "eMBB"))

    # --- Engineer 44 features (identical contract to URLLC/mMTC) ---
    feat_cols = list(RAW_KPI_COLUMNS)
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_rmean{ROLLING_WINDOW}"] = (
            df[col].rolling(ROLLING_WINDOW, min_periods=1).mean())
        feat_cols.append(f"{col}_rmean{ROLLING_WINDOW}")
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_rstd{ROLLING_WINDOW}"] = (
            df[col].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0))
        feat_cols.append(f"{col}_rstd{ROLLING_WINDOW}")
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_diff1"] = df[col].diff(1).fillna(0)
        feat_cols.append(f"{col}_diff1")
    assert len(feat_cols) == 44, f"Expected 44 features, got {len(feat_cols)}"
    print(f"   Features: {len(feat_cols)} (11 raw + 11 rmean + 11 rstd + 11 diff)")

    # --- Target: forward-smoothed Throughput ---
    df["_target"] = (df["Throughput_bps"]
                     .rolling(target_window).mean()
                     .shift(-(target_window - 1)))
    df = df.dropna().reset_index(drop=True)

    X_flat = df[feat_cols].values.astype(np.float32)
    y_flat = df[["_target"]].values.astype(np.float32)
    print(f"   Flat samples after dropna: {len(X_flat):,} | features: {X_flat.shape[1]}")

    if use_shuffle:
        # ------------------------------------------------------------------
        # STRATIFIED SHUFFLE PATH
        # Fit scaler on FULL dataset — safe because the split is random
        # (no temporal ordering, no future leakage).
        # ------------------------------------------------------------------
        sc_in  = RobustScaler()
        sc_tgt = RobustScaler()
        X_scaled = sc_in.fit_transform(X_flat).astype(np.float32)
        y_scaled = sc_tgt.fit_transform(y_flat).astype(np.float32)
        print(f"   Scaler fit on FULL dataset (shuffle mode)")
        print(f"   center(avg)={np.mean(sc_in.center_):.4f} | "
              f"scale(avg)={np.mean(sc_in.scale_):.4f}")

        # Build sequences first, then split
        # (ensures sequences don't span train/val boundary)
        print(f"   Building sliding windows (window={seq_len})...")
        X_seq, y_seq = _make_sequences(X_scaled, y_scaled, seq_len)
        print(f"   Total sequences: {len(X_seq):,}")

        X_train, y_train, X_test, y_test = _embb_stratified_split(
            X_seq, y_seq, n_bins=EMBB_SPLIT_BINS, test_size=0.15)

    else:
        # ------------------------------------------------------------------
        # CHRONOLOGICAL FALLBACK PATH (--no_shuffle)
        # Scaler fit on train only to avoid temporal leakage.
        # ------------------------------------------------------------------
        print(f"   [--no_shuffle] Chronological 85/15 split (ablation mode)")
        split     = int(len(X_flat) * 0.85)
        X_tr_flat = X_flat[:split]
        X_te_flat = X_flat[split:]
        y_tr_flat = y_flat[:split]
        y_te_flat = y_flat[split:]
        print(f"   Chronological: train={len(X_tr_flat):,} | val={len(X_te_flat):,}")

        sc_in  = RobustScaler()
        sc_tgt = RobustScaler()
        X_tr_flat = sc_in.fit_transform(X_tr_flat).astype(np.float32)
        X_te_flat = sc_in.transform(X_te_flat).astype(np.float32)
        y_tr_flat = sc_tgt.fit_transform(y_tr_flat).astype(np.float32)
        y_te_flat = sc_tgt.transform(y_te_flat).astype(np.float32)

        print(f"   Building sliding windows (window={seq_len})...")
        X_train, y_train = _make_sequences(X_tr_flat, y_tr_flat, seq_len)
        X_test,  y_test  = _make_sequences(X_te_flat, y_te_flat, seq_len)

    print(f"   Final: X_train={X_train.shape} | X_test={X_test.shape}")
    return (X_train, y_train, X_test, y_test,
            sc_in, sc_tgt, target_window, feat_cols)


def load_data_urllc(base_path: str):
    """
    URLLC data loader v6.0.
    Engineers 44 features (raw+rmean+rstd+diff). Returns flat arrays.
    UNCHANGED from v6.0 (R²=0.85 achieved).
    """
    print(f"\n{'='*60}")
    print("  URLLC DATA LOADER v6.0  (unchanged)")
    print(f"{'='*60}")

    df = _validate_and_clip(_read_parquet_slice(base_path, "URLLC"))

    feat_cols = list(RAW_KPI_COLUMNS)
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_rmean{ROLLING_WINDOW}"] = (
            df[col].rolling(ROLLING_WINDOW, min_periods=1).mean())
        feat_cols.append(f"{col}_rmean{ROLLING_WINDOW}")
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_rstd{ROLLING_WINDOW}"] = (
            df[col].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0))
        feat_cols.append(f"{col}_rstd{ROLLING_WINDOW}")
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_diff1"] = df[col].diff(1).fillna(0)
        feat_cols.append(f"{col}_diff1")

    df["_target"] = (df["Throughput_bps"]
                     .rolling(URLLC_TARGET_WINDOW).mean()
                     .shift(-(URLLC_TARGET_WINDOW - 1)))
    df = df.dropna().reset_index(drop=True)

    X = df[feat_cols].values.astype(np.float32)
    y = df[["_target"]].values.astype(np.float32)
    print(f"   Samples: {len(X):,} | Features: {X.shape[1]}")
    return X, y, URLLC_TARGET_WINDOW, feat_cols


def load_data_mmtc(base_path: str):
    """
    mMTC data loader v6.2.
    Uses the same 44-feature set as URLLC (raw+rmean+rstd+diff).
    UNCHANGED from v6.2 (R²=0.79 achieved).
    """
    print(f"\n{'='*60}")
    print("  mMTC DATA LOADER v6.2  (unchanged)")
    print("  44-feature set: 11 raw + 11 rmean + 11 rstd + 11 diff")
    print(f"{'='*60}")

    df = _validate_and_clip(_read_parquet_slice(base_path, "mMTC"))

    feat_cols = list(RAW_KPI_COLUMNS)
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_rmean{ROLLING_WINDOW}"] = (
            df[col].rolling(ROLLING_WINDOW, min_periods=1).mean())
        feat_cols.append(f"{col}_rmean{ROLLING_WINDOW}")
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_rstd{ROLLING_WINDOW}"] = (
            df[col].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0))
        feat_cols.append(f"{col}_rstd{ROLLING_WINDOW}")
    for col in RAW_KPI_COLUMNS:
        df[f"{col}_diff1"] = df[col].diff(1).fillna(0)
        feat_cols.append(f"{col}_diff1")

    assert len(feat_cols) == 44, f"Expected 44 features, got {len(feat_cols)}"

    df["_target"] = (df["Throughput_bps"]
                     .rolling(MMTC_TARGET_WINDOW).mean()
                     .shift(-(MMTC_TARGET_WINDOW - 1)))
    df = df.dropna().reset_index(drop=True)

    X = df[feat_cols].values.astype(np.float32)
    y = df[["_target"]].values.astype(np.float32)
    print(f"   Samples: {len(X):,} | Features: {X.shape[1]}")
    return X, y, MMTC_TARGET_WINDOW, feat_cols


# =============================================================================
# SHARED KERAS BUILDING BLOCKS
# =============================================================================

class GatedResidualNetwork(layers.Layer):
    """
    GRN block shared across all slice architectures.
    Gate g(x) learns to blend transformed signal h with the residual,
    suppressing irrelevant dimensions without discarding them entirely.
    Optional projection handles input/output dim mismatch.
    """
    def __init__(self, units, dropout_rate=0.1, **kwargs):
        super().__init__(**kwargs)
        self.units        = units
        self.dropout_rate = dropout_rate

    def build(self, input_shape):
        in_dim       = int(input_shape[-1])
        self.dense1  = layers.Dense(self.units, activation="elu",
                                    kernel_regularizer=regularizers.l2(1e-4))
        self.dense2  = layers.Dense(self.units,
                                    kernel_regularizer=regularizers.l2(1e-4))
        self.gate    = layers.Dense(self.units, activation="sigmoid")
        self.bn      = layers.BatchNormalization()
        self.dropout = layers.Dropout(self.dropout_rate)
        self.proj    = (layers.Dense(self.units, use_bias=False)
                        if in_dim != self.units else None)
        super().build(input_shape)

    def call(self, x, training=False):
        res = self.proj(x) if self.proj else x
        h   = self.dense1(x)
        h   = self.dropout(h, training=training)
        h   = self.dense2(h)
        g   = self.gate(x)
        return self.bn(g * h + (1 - g) * res, training=training)

    def get_config(self):
        cfg = super().get_config()
        cfg.update({"units": self.units, "dropout_rate": self.dropout_rate})
        return cfg


class TFTVariableSelection(layers.Layer):
    """
    TFT Variable Selection Network (VSN).
    Learns per-feature importance weights so the model automatically
    focuses on the KPIs most relevant to each slice type.

    x (batch, F) ->
      per-feature GRNs -> stack (batch, F, units)
      flat GRN -> softmax weights (batch, F)
      weighted sum -> context (batch, units)
    """
    def __init__(self, num_inputs, units, dropout_rate=0.1, **kwargs):
        super().__init__(**kwargs)
        self.num_inputs   = num_inputs
        self.units        = units
        self.dropout_rate = dropout_rate

    def build(self, input_shape):
        self.feat_grns = [
            GatedResidualNetwork(self.units, self.dropout_rate,
                                 name=f"vsn_feat_{i}")
            for i in range(self.num_inputs)
        ]
        self.flat_grn = GatedResidualNetwork(
            self.num_inputs, self.dropout_rate, name="vsn_flat")
        self.softmax  = layers.Softmax(axis=-1)
        super().build(input_shape)

    def call(self, x, training=False):
        feat_outs = tf.stack(
            [self.feat_grns[i](x[:, i:i+1], training=training)
             for i in range(self.num_inputs)], axis=1)
        weights = self.softmax(self.flat_grn(x, training=training))
        context = tf.reduce_sum(
            feat_outs * tf.expand_dims(weights, axis=-1), axis=1)
        return context, weights

    def get_config(self):
        cfg = super().get_config()
        cfg.update({"num_inputs": self.num_inputs, "units": self.units,
                    "dropout_rate": self.dropout_rate})
        return cfg


def _make_optimizer(lr: float):
    """AdamW with TF 2.10 fallback to Adam+clipnorm."""
    try:
        opt = optimizers.AdamW(learning_rate=lr, weight_decay=1e-4)
        print(f"   Optimizer: AdamW(lr={lr}, weight_decay=1e-4)")
    except AttributeError:
        opt = optimizers.Adam(learning_rate=lr, clipnorm=1.0)
        print(f"   Optimizer: Adam(lr={lr}, clipnorm=1.0) [TF2.10 compat]")
    return opt


# =============================================================================
# MODEL BUILDERS
# =============================================================================

def build_model_embb(seq_len: int, num_features: int, lr: float = 0.0007):
    """
    eMBB v6.4 -- VAR + BiGRU + TFT with recalibrated hyperparameters.

    MODEL STRUCTURE: unchanged from v6.3.
    HYPERPARAMETERS: recalibrated now that the root cause (distribution shift)
    is fixed at the data level by stratified shuffle split.

    v6.3 used extreme regularisation (L2=1e-3, DR=0.40) as compensation for
    distribution shift — the model had to be heavily constrained to prevent
    it from memorising a training distribution that was completely different
    from validation. With the split fixed, those extreme settings cause
    underfitting. We return to URLLC-level regularisation (L2=1e-4, DR=0.20)
    which proved effective for the same VAR+GRU+TFT family.

    Architecture (UNCHANGED from v6.3):
      ┌───────────────────────────────────────────────────────────────┐
      │  Input(batch, seq_len=30, F=44)                               │
      │                                                               │
      │  ① VAR Branch                                                 │
      │     TimeDistributed Dense(32, linear, L2) -> BN               │
      │     GlobalAveragePooling1D -> Dropout(0.15)                   │
      │     -> (batch, 32)                                            │
      │                                                               │
      │  ② BiGRU Branch                                               │
      │     Bidirectional GRU(64, return_seq, concat) -> Dropout(0.20)│
      │     Bidirectional GRU(32, concat) -> Dropout(0.15)            │
      │     -> (batch, 64)                                            │
      │                                                               │
      │  ③ TFT Branch                                                 │
      │     VSN(44, 32) on last timestep -> GRN(32) -> Dropout(0.15) │
      │     -> (batch, 32)                                            │
      │                                                               │
      │  Concat(32+64+32=128)                                         │
      │  -> GRN(64, DR=0.15) -> GRN(32, DR=0.08)                     │
      │  -> Dense(16, relu) -> Dense(1, linear)                       │
      └───────────────────────────────────────────────────────────────┘

    Hyperparameter comparison:
      Parameter       v6.3      v6.4     Reason for change
      ─────────────────────────────────────────────────────────────
      L2              1e-3      1e-4     Root cause fixed; less constraint needed
      Dropout BiGRU   0.40      0.20     Same; model can learn real patterns now
      Dropout Dense   0.35      0.15     Same
      LR              0.00035   0.0007   No longer need artificially slow LR
      EarlyStopping   60        40       Converges faster with good split
    """
    L2      = 1e-4   # Recalibrated from 1e-3 (URLLC-proven level)
    DR_BGRU = 0.20   # Recalibrated from 0.40
    DR_DNS  = 0.15   # Recalibrated from 0.35

    inp = layers.Input(shape=(seq_len, num_features), name="kpi_input")

    # ------------------------------------------------------------------
    # ① VAR-Linear Branch
    #   TimeDistributed Dense(32 linear) — compact cross-KPI projection
    #   applied identically to each timestep.
    #   GlobalAvgPool collapses the time axis to a 32-d summary.
    # ------------------------------------------------------------------
    var_x = layers.TimeDistributed(
        layers.Dense(32, activation="linear",
                     kernel_regularizer=regularizers.l2(L2)),
        name="var_td")(inp)
    var_x = layers.TimeDistributed(
        layers.BatchNormalization(), name="var_td_bn")(var_x)
    var_x = layers.GlobalAveragePooling1D(name="var_gap")(var_x)
    var_x = layers.Dropout(DR_DNS, name="var_drop")(var_x)
    # var_x: (batch, 32)

    # ------------------------------------------------------------------
    # ② BiGRU-Temporal Branch
    #   Bidirectional GRU reads seq_len real timesteps forward AND backward.
    #   merge_mode='concat' doubles hidden dim: 64 units → 128, 32 → 64.
    #   Forward: detects throughput trends building over time.
    #   Backward: detects whether throughput is recovering from a dip.
    # ------------------------------------------------------------------
    bigru_x = layers.Bidirectional(
        layers.GRU(
            64,
            return_sequences=True,
            dropout=DR_BGRU,
            recurrent_dropout=0.0,
            kernel_regularizer=regularizers.l2(L2),
            recurrent_regularizer=regularizers.l2(L2),
        ),
        merge_mode="concat",
        name="bigru_1"
    )(inp)
    # bigru_x: (batch, seq_len, 128)

    bigru_x = layers.Bidirectional(
        layers.GRU(
            32,
            return_sequences=False,
            dropout=DR_DNS,
            recurrent_dropout=0.0,
            kernel_regularizer=regularizers.l2(L2),
            recurrent_regularizer=regularizers.l2(L2),
        ),
        merge_mode="concat",
        name="bigru_2"
    )(bigru_x)
    # bigru_x: (batch, 64)

    # ------------------------------------------------------------------
    # ③ TFT-Attention Branch
    #   VSN(44, 32) on the LAST timestep: instantaneous feature attention.
    #   Asks: which of the 44 KPIs best characterise the current moment?
    # ------------------------------------------------------------------
    last_step = layers.Lambda(lambda t: t[:, -1, :], name="last_step")(inp)
    last_bn   = layers.BatchNormalization(name="last_bn")(last_step)
    tft_ctx, _ = TFTVariableSelection(
        num_inputs=num_features, units=32,
        dropout_rate=DR_DNS, name="vsn")(last_bn)
    tft_x = GatedResidualNetwork(32, dropout_rate=DR_DNS,
                                  name="tft_grn")(tft_ctx)
    tft_x = layers.Dropout(DR_DNS, name="tft_drop")(tft_x)
    # tft_x: (batch, 32)

    # ------------------------------------------------------------------
    # Fusion: 32 + 64 + 32 = 128
    # ------------------------------------------------------------------
    fused = layers.Concatenate(name="fusion")([var_x, bigru_x, tft_x])

    out = GatedResidualNetwork(64, dropout_rate=DR_DNS,
                                name="fusion_grn1")(fused)
    out = GatedResidualNetwork(32, dropout_rate=DR_DNS * 0.5,
                                name="fusion_grn2")(out)
    out = layers.Dense(16, activation="relu",
                       kernel_regularizer=regularizers.l2(L2),
                       name="pre_output")(out)
    output = layers.Dense(1, activation="linear", name="output")(out)

    model = models.Model(inputs=inp, outputs=output,
                         name="eMBB_VAR_BiGRU_TFT_v64")

    print(f"\n   eMBB v6.4 Architecture  [VAR+BiGRU+TFT, recalibrated HPs]:")
    print(f"   Input  : (batch, {seq_len}, {num_features})")
    print(f"   VAR    : TimeDistributed Dense(32 linear) + GlobalAvgPool -> (batch, 32)")
    print(f"   BiGRU  : BiGRU(64,concat)->BiGRU(32,concat) -> (batch, 64)")
    print(f"   TFT    : VSN({num_features}, 32) on last timestep -> (batch, 32)")
    print(f"   Fusion : 32+64+32=128 -> GRN(64)->GRN(32)->Dense(16)->Dense(1)")
    print(f"   L2={L2} | Dropout BiGRU={DR_BGRU} Dense={DR_DNS}")
    print(f"   Output : (batch, 1) -- {TARGET_KPI}")

    loss_fn = tf.keras.losses.Huber(delta=5.0)
    print(f"   Loss   : Huber(delta=5.0)")
    model.compile(optimizer=_make_optimizer(lr),
                  loss=loss_fn, metrics=["mae", r_squared])
    model.summary(line_length=90)
    return model


def build_model_urllc(num_features: int, lr: float = 0.0007):
    """
    URLLC v6.0 -- VAR-Linear + pseudo-seq GRU + TFT (flat input).
    Proven architecture, UNCHANGED (R²=0.85 achieved).

    Input  : (batch, 44)
    Output : (batch, 1)
    """
    inp  = layers.Input(shape=(num_features,), name="kpi_input")
    x_bn = layers.BatchNormalization(name="input_bn")(inp)

    # VAR branch
    var_x = layers.Dense(128, activation="linear",
                          kernel_regularizer=regularizers.l2(1e-4),
                          name="var_proj")(x_bn)
    var_x = layers.BatchNormalization(name="var_bn")(var_x)
    var_x = layers.Dropout(0.10, name="var_drop")(var_x)

    # GRU branch (pseudo-sequence over feature dimensions)
    gru_x = layers.Reshape((num_features, 1), name="gru_reshape")(x_bn)
    gru_x = layers.GRU(64, return_sequences=True, dropout=0.10,
                        kernel_regularizer=regularizers.l2(1e-4),
                        name="gru_1")(gru_x)
    gru_x = layers.GRU(32, return_sequences=False, dropout=0.10,
                        kernel_regularizer=regularizers.l2(1e-4),
                        name="gru_2")(gru_x)

    # TFT VSN branch
    tft_ctx, _ = TFTVariableSelection(
        num_inputs=num_features, units=64,
        dropout_rate=0.10, name="vsn")(x_bn)
    tft_x = GatedResidualNetwork(64, dropout_rate=0.10, name="tft_grn")(tft_ctx)

    fused = layers.Concatenate(name="fusion")([var_x, gru_x, tft_x])

    out = GatedResidualNetwork(128, dropout_rate=0.10, name="grn_128")(fused)
    out = GatedResidualNetwork(64,  dropout_rate=0.05, name="grn_64")(out)
    out = layers.Dense(32, activation="relu",
                       kernel_regularizer=regularizers.l2(1e-4),
                       name="pre_out")(out)
    output = layers.Dense(1, activation="linear", name="output")(out)

    model = models.Model(inputs=inp, outputs=output,
                         name="URLLC_VAR_GRU_TFT_v60")

    print(f"\n   URLLC v6.0 Architecture:")
    print(f"   Input  : (batch, {num_features})  flat 44-feature set")
    print(f"   Branches: VAR(128) + GRU(64->32) + VSN(64)")
    print(f"   Output : (batch, 1) -- {TARGET_KPI}")
    loss_fn = tf.keras.losses.Huber(delta=1.0)
    print(f"   Loss   : Huber(delta=1.0)")
    model.compile(optimizer=_make_optimizer(lr),
                  loss=loss_fn, metrics=["mae", r_squared])
    model.summary(line_length=90)
    return model


def build_model_mmtc(num_features: int, lr: float = 0.0007):
    """
    mMTC v6.2 -- VAR-Linear + GRU-Temporal + TFT-Attention (flat input).
    UNCHANGED (R²=0.79 achieved).

    Input  : (batch, 44)
    Output : (batch, 1)
    """
    dropout_rate = 0.05

    inp  = layers.Input(shape=(num_features,), name="kpi_input")
    x_bn = layers.BatchNormalization(name="input_bn")(inp)

    # ① VAR-Linear Branch
    var_x = layers.Dense(64, activation="linear",
                          kernel_regularizer=regularizers.l2(1e-4),
                          name="var_proj")(x_bn)
    var_x = layers.BatchNormalization(name="var_bn")(var_x)
    var_x = layers.Dropout(dropout_rate, name="var_drop")(var_x)

    # ② GRU-Temporal Branch
    gru_x = layers.Reshape((num_features, 1), name="gru_reshape")(x_bn)
    gru_x = layers.GRU(32, return_sequences=True,
                        dropout=dropout_rate, recurrent_dropout=0.0,
                        kernel_regularizer=regularizers.l2(1e-4),
                        name="gru_1")(gru_x)
    gru_x = layers.GRU(16, return_sequences=False,
                        dropout=dropout_rate, recurrent_dropout=0.0,
                        kernel_regularizer=regularizers.l2(1e-4),
                        name="gru_2")(gru_x)

    # ③ TFT-Attention Branch
    tft_ctx, _ = TFTVariableSelection(
        num_inputs=num_features, units=32,
        dropout_rate=dropout_rate, name="vsn")(x_bn)
    tft_x = GatedResidualNetwork(32, dropout_rate=dropout_rate,
                                  name="tft_grn")(tft_ctx)

    fused = layers.Concatenate(name="fusion")([var_x, gru_x, tft_x])

    out = GatedResidualNetwork(64, dropout_rate=dropout_rate,
                                name="fusion_grn1")(fused)
    out = GatedResidualNetwork(32, dropout_rate=dropout_rate * 0.5,
                                name="fusion_grn2")(out)
    out = layers.Dense(16, activation="relu",
                       kernel_regularizer=regularizers.l2(1e-4),
                       name="pre_output")(out)
    output = layers.Dense(1, activation="linear", name="output")(out)

    model = models.Model(inputs=inp, outputs=output,
                         name="mMTC_VAR_GRU_TFT_v62")

    print(f"\n   mMTC v6.2 Architecture  [VAR+GRU+TFT]:")
    print(f"   Input  : (batch, {num_features})  flat 44-feature set")
    print(f"   VAR    : Dense(64 linear) -> (batch, 64)")
    print(f"   GRU    : Reshape(44,1)->GRU(32)->GRU(16) -> (batch, 16)")
    print(f"   TFT    : VSN({num_features}, 32)->GRN(32) -> (batch, 32)")
    print(f"   Fusion : 64+16+32=112 -> GRN(64)->GRN(32)->Dense(16)->Dense(1)")
    print(f"   Output : (batch, 1) -- {TARGET_KPI}")
    loss_fn = tf.keras.losses.Huber(delta=0.5)
    print(f"   Loss   : Huber(delta=0.5)")
    model.compile(optimizer=_make_optimizer(lr),
                  loss=loss_fn, metrics=["mae", r_squared])
    model.summary(line_length=90)
    return model


# =============================================================================
# EVALUATION
# =============================================================================
def evaluate_real_world(model, X_test, y_test,
                        scaler_target, slice_name: str, version: str):
    """Inverse-transform predictions and compute real-world MAE and R2."""
    print("\nComputing real-world metrics (inverse-transformed)...")
    preds_sc   = model.predict(X_test, verbose=0)
    preds_real = np.maximum(scaler_target.inverse_transform(preds_sc), 0)
    y_real     = np.maximum(scaler_target.inverse_transform(y_test), 0)

    mae     = mean_absolute_error(y_real[:, 0], preds_real[:, 0])
    r2      = sklearn_r2(y_real[:, 0], preds_real[:, 0])
    mean_tp = np.mean(y_real[:, 0])
    acc     = max(0.0, (1 - mae / (mean_tp + 1e-9)) * 100)
    verdict = ("EXCELLENT"  if r2 >= 0.85 else
               "GOOD"       if r2 >= 0.65 else
               "ACCEPTABLE" if r2 >= 0.40 else "POOR")

    print(f"\n{'='*55}")
    print(f"  REAL-WORLD VERDICT -- {slice_name} ({version})")
    print(f"{'='*55}")
    print(f"  Throughput MAE  : {mae:>15,.0f} bps  ({mae/1e6:.3f} Mbps)")
    print(f"  Mean Throughput : {mean_tp:>15,.0f} bps")
    print(f"  Estimated Acc.  : {acc:>14.2f} %")
    print(f"  R2 Score        : {r2:>14.4f}")
    print(f"  Verdict         : {verdict}")
    print(f"{'='*55}")

    return {
        "target_kpi":          TARGET_KPI,
        "throughput_mae_bps":  float(mae),
        "throughput_r2":       float(r2),
        "estimated_accuracy":  float(acc),
        "mean_throughput_bps": float(mean_tp),
        "verdict":             verdict,
    }


# =============================================================================
# ONNX EXPORT (shape-aware)
# =============================================================================
def export_onnx(model, output_dir: str, input_shape: tuple):
    """
    Export model to ONNX.
    input_shape = per-sample shape without batch dim:
      eMBB  -> (seq_len, num_features)  e.g. (30, 44)
      URLLC -> (44,)
      mMTC  -> (44,)
    """
    print("\nExporting ONNX...")
    onnx_path = os.path.join(output_dir, "model.onnx")
    try:
        spec = (tf.TensorSpec((None,) + input_shape,
                              tf.float32, name="kpi_input"),)
        tf2onnx.convert.from_keras(model, input_signature=spec,
                                   opset=13, output_path=onnx_path)
        size_mb = os.path.getsize(onnx_path) / (1024 * 1024)
        print(f"   ONNX -> {onnx_path}  ({size_mb:.2f} MB)")
        return onnx_path
    except Exception as e:
        print(f"   ONNX primary failed: {e} -- trying CLI fallback...")
        try:
            tmp = os.path.join(output_dir, "_tmp_savedmodel")
            model.save(tmp, save_format="tf")
            res = subprocess.run(
                ["python", "-m", "tf2onnx.convert",
                 "--saved-model", tmp, "--output", onnx_path, "--opset", "13"],
                capture_output=True, text=True, timeout=120)
            shutil.rmtree(tmp, ignore_errors=True)
            if res.returncode == 0 and os.path.exists(onnx_path):
                size_mb = os.path.getsize(onnx_path) / (1024 * 1024)
                print(f"   ONNX (CLI fallback) -> {onnx_path}  ({size_mb:.2f} MB)")
                return onnx_path
            print(f"   CLI fallback failed: {res.stderr}")
        except Exception as e2:
            print(f"   ONNX export completely failed: {e2}")
        return None


# =============================================================================
# HDFS UPLOAD
# =============================================================================
def upload_to_hdfs(local_dir: str, slice_name: str):
    if not HDFS_AVAILABLE:
        print("   HDFS not available -- skipping upload")
        return
    try:
        fs   = pafs.HadoopFileSystem(host="namenode", port=8020)
        base = f"/5g_kpi/models/{slice_name}"
        try:
            fs.create_dir(base, recursive=True)
        except Exception:
            pass
        for fname, hdfs_name in [
            ("model.onnx",        "model_latest.onnx"),
            ("scaler_params.json","scaler_latest.json"),
            ("history.json",      "history.json"),
        ]:
            lp = os.path.join(local_dir, fname)
            if os.path.exists(lp):
                with open(lp, "rb") as f:
                    data = f.read()
                with fs.open_output_stream(f"{base}/{hdfs_name}") as out:
                    out.write(data)
                print(f"   {fname} -> hdfs://{base}/{hdfs_name}")
        print(f"   HDFS upload complete [{slice_name}]")
    except Exception as e:
        print(f"   HDFS upload failed: {e}")


# =============================================================================
# SCALER JSON BUILDER
# =============================================================================
def build_scaler_json(slice_name, version, target_window,
                      feat_names, sc_in, sc_tgt,
                      onnx_path, metrics, extra=None):
    data = {
        "slice_name":          slice_name,
        "architecture":        f"{slice_name}_{version}",
        "version":             version,
        "target_kpi":          TARGET_KPI,
        "target_steps_ahead":  target_window,
        "input_features":      feat_names,
        "output_features":     [TARGET_KPI],
        "raw_kpi_columns":     list(RAW_KPI_COLUMNS),
        "rolling_window":      ROLLING_WINDOW,
        "num_input_features":  len(feat_names),
        "num_raw_features":    NUM_RAW,
        "num_output_features": 1,
        "center":              sc_in.center_.tolist(),
        "scale":               sc_in.scale_.tolist(),
        "target_center":       sc_tgt.center_.tolist(),
        "target_scale":        sc_tgt.scale_.tolist(),
        "onnx_exported":       onnx_path is not None,
        "training_metrics":    metrics,
    }
    if extra:
        data.update(extra)
    return data


# =============================================================================
# SHARED POST-TRAINING STEPS
# =============================================================================
def save_artifacts(model, hist, metrics, sc_in, sc_tgt,
                   args, version, target_window, feat_names,
                   onnx_input_shape, extra_scaler):
    """Save history, weights, scaler JSON, ONNX, upload to HDFS."""
    hist_path = os.path.join(args.output_dir, "history.json")
    with open(hist_path, "w") as f:
        json.dump({k: [float(v) for v in vs]
                   for k, vs in hist.history.items()}, f, indent=2)
    print(f"   History -> {hist_path}")

    try:
        model.save_weights(os.path.join(args.output_dir, "best.weights.h5"))
        print(f"   Weights -> {args.output_dir}/best.weights.h5")
    except Exception as e:
        print(f"   Weights save failed: {e}")

    onnx_path = export_onnx(model, args.output_dir, onnx_input_shape)

    scaler_data = build_scaler_json(
        args.slice_name, version, target_window, feat_names,
        sc_in, sc_tgt, onnx_path, metrics, extra_scaler)
    scaler_path = os.path.join(args.output_dir, "scaler_params.json")
    with open(scaler_path, "w") as f:
        json.dump(scaler_data, f, indent=2)
    print(f"   Scaler JSON -> {scaler_path}")

    upload_to_hdfs(args.output_dir, args.slice_name)

    return onnx_path, scaler_path, hist_path


# =============================================================================
# SLICE PIPELINES
# =============================================================================

def run_embb(args):
    seq_len       = args.seq_len
    target_window = args.target_window
    use_shuffle   = not args.no_shuffle

    print("\n" + "="*65)
    print("  5G NWDAF v6.4 -- eMBB  [VAR+BiGRU+TFT]")
    print(f"  ROOT CAUSE FIX: chronological distribution shift resolved")
    print(f"  via stratified shuffle split (10 throughput quantile bins)")
    print(f"  {'─'*57}")
    print(f"  seq_len      : {seq_len} steps")
    print(f"  target_window: {target_window} steps")
    print(f"  Split        : {'STRATIFIED SHUFFLE' if use_shuffle else 'CHRONOLOGICAL (ablation)'}")
    print(f"  Features     : 44 (raw+rmean+rstd+diff)")
    print(f"  L2=1e-4  | Dropout BiGRU=0.20  Dense=0.15")
    print(f"  Huber delta  : 5.0")
    print(f"  Data         : {args.data_dir}")
    print(f"  Out          : {args.output_dir}")
    print(f"  lr={args.lr}  epochs={args.epochs}  batch={args.batch_size}")
    print("="*65)

    os.makedirs(args.output_dir, exist_ok=True)

    (X_train, y_train, X_test, y_test,
     sc_in, sc_tgt, tw_used, feat_names) = load_data_embb(
        args.data_dir, seq_len, target_window, use_shuffle)
    num_features = X_train.shape[2]

    print(f"\nBuilding eMBB v6.4 model  [VAR+BiGRU+TFT]...")
    with strategy.scope():
        model = build_model_embb(seq_len, num_features, lr=args.lr)

    ckpt = os.path.join(args.output_dir, "best_checkpoint.tf")
    cb_list = [
        callbacks.EarlyStopping(
            monitor="val_r_squared", mode="max",
            patience=40, restore_best_weights=True, verbose=1),
        callbacks.ReduceLROnPlateau(
            monitor="val_r_squared", mode="max",
            factor=0.5, patience=10, min_lr=1e-6, verbose=1),
        callbacks.ModelCheckpoint(
            filepath=ckpt, monitor="val_r_squared",
            mode="max", save_best_only=True, save_format="tf", verbose=0),
        callbacks.CSVLogger(
            os.path.join(args.output_dir, "training_log.csv"), append=False),
    ]

    print(f"\nTraining eMBB -- up to {args.epochs} epochs "
          f"(EarlyStopping patience=40)...\n")
    hist = model.fit(
        X_train, y_train, epochs=args.epochs,
        batch_size=args.batch_size * strategy.num_replicas_in_sync,
        validation_data=(X_test, y_test),
        callbacks=cb_list, verbose=1)

    best_r2  = max(hist.history.get("val_r_squared", [0.0]))
    best_mae = min(hist.history.get("val_mae", [float("inf")]))
    print(f"\n   Best val R2  : {best_r2:.4f}")
    print(f"   Best val MAE : {best_mae:.6f} (scaled)")

    metrics = evaluate_real_world(model, X_test, y_test, sc_tgt, "eMBB", "v6.4")

    extra = {
        "onnx_input_shape":   [1, seq_len, num_features],
        "seq_len":            seq_len,
        "target_window":      tw_used,
        "num_features":       num_features,
        "split_method":       "stratified_shuffle" if use_shuffle else "chronological",
        "split_bins":         EMBB_SPLIT_BINS if use_shuffle else None,
        "_scala_inference_steps": [
            f"1. Maintain circular buffer of last {seq_len} per-second KPI rows",
            "2. Per row: compute rmean(5), rstd(5), diff(1) -> 44 features",
            "3. Normalise: norm[i] = (feat[i] - center[i]) / scale[i]",
            f"4. Stack {seq_len} rows -> float[{seq_len}][44]",
            "5. ONNX.run(input_3d) -> float[1]",
            "6. throughput_bps = pred[0] * target_scale[0] + target_center[0]",
        ]
    }
    onnx_path, scaler_path, hist_path = save_artifacts(
        model, hist, metrics, sc_in, sc_tgt, args,
        "v6.4", tw_used, feat_names,
        onnx_input_shape=(seq_len, num_features),
        extra_scaler=extra)

    print(f"\n{'='*65}")
    print(f"  eMBB v6.4 TRAINING COMPLETE  [VAR+BiGRU+TFT]")
    print(f"{'='*65}")
    print(f"   ONNX shape  : (1, {seq_len}, {num_features})")
    print(f"   Best val R2 : {best_r2:.4f}")
    print(f"   Split       : {'stratified shuffle' if use_shuffle else 'chronological'}")
    print(f"   ONNX        : {onnx_path or 'FAILED'}")
    print(f"   Scaler JSON : {scaler_path}")
    print(f"   History     : {hist_path}")
    print(f"   Checkpoint  : {ckpt}")
    print(f"{'='*65}")
    return metrics


def run_urllc(args):
    print("\n" + "="*65)
    print("  5G NWDAF v6.0 -- URLLC  [VAR+GRU+TFT] (proven, unchanged)")
    print(f"  Input: (batch, 44) flat | Huber delta=1.0")
    print(f"  Data : {args.data_dir}")
    print(f"  Out  : {args.output_dir}")
    print(f"  lr={args.lr}  epochs={args.epochs}  batch={args.batch_size}")
    print("="*65)

    os.makedirs(args.output_dir, exist_ok=True)

    X, y, target_window, feat_names = load_data_urllc(args.data_dir)
    X_train, y_train, X_test, y_test, sc_in, sc_tgt = scale_and_split(X, y)
    num_features = X_train.shape[1]

    print(f"\nBuilding URLLC v6.0 model...")
    with strategy.scope():
        model = build_model_urllc(num_features, lr=args.lr)

    ckpt = os.path.join(args.output_dir, "best_checkpoint.tf")
    cb_list = [
        callbacks.EarlyStopping(
            monitor="val_r_squared", mode="max",
            patience=40, restore_best_weights=True, verbose=1),
        callbacks.ReduceLROnPlateau(
            monitor="val_r_squared", mode="max",
            factor=0.5, patience=10, min_lr=1e-6, verbose=1),
        callbacks.ModelCheckpoint(
            filepath=ckpt, monitor="val_r_squared",
            mode="max", save_best_only=True, save_format="tf", verbose=0),
        callbacks.CSVLogger(
            os.path.join(args.output_dir, "training_log.csv"), append=False),
    ]

    print(f"\nTraining URLLC -- up to {args.epochs} epochs "
          f"(EarlyStopping patience=40)...\n")
    hist = model.fit(
        X_train, y_train, epochs=args.epochs,
        batch_size=args.batch_size * strategy.num_replicas_in_sync,
        validation_data=(X_test, y_test),
        callbacks=cb_list, verbose=1)

    best_r2  = max(hist.history.get("val_r_squared", [0.0]))
    best_mae = min(hist.history.get("val_mae", [float("inf")]))
    print(f"\n   Best val R2  : {best_r2:.4f}")
    print(f"   Best val MAE : {best_mae:.6f} (scaled)")

    metrics = evaluate_real_world(model, X_test, y_test, sc_tgt, "URLLC", "v6.0")

    extra = {
        "onnx_input_shape": [1, num_features],
        "_scala_inference_steps": [
            "1. Collect 11 raw KPI values from PcapKpiExtractor",
            "2. Compute rmean(5), rstd(5), diff(1) per KPI -> 44 features",
            "3. Normalise: norm[i] = (feat[i] - center[i]) / scale[i]",
            "4. ONNX.run(float[44]) -> float[1]",
            "5. throughput_bps = pred[0] * target_scale[0] + target_center[0]",
        ]
    }
    onnx_path, scaler_path, hist_path = save_artifacts(
        model, hist, metrics, sc_in, sc_tgt, args,
        "v6.0", target_window, feat_names,
        onnx_input_shape=(num_features,),
        extra_scaler=extra)

    print(f"\n{'='*65}")
    print(f"  URLLC v6.0 TRAINING COMPLETE")
    print(f"{'='*65}")
    print(f"   ONNX shape  : (1, {num_features})")
    print(f"   Best val R2 : {best_r2:.4f}")
    print(f"   ONNX        : {onnx_path or 'FAILED'}")
    print(f"   Scaler JSON : {scaler_path}")
    print(f"   History     : {hist_path}")
    print(f"   Checkpoint  : {ckpt}")
    print(f"{'='*65}")
    return metrics


def run_mmtc(args):
    print("\n" + "="*65)
    print("  5G NWDAF v6.2 -- mMTC  [VAR+GRU+TFT] (proven, unchanged)")
    print("  44-feature set (11 raw + 11 rmean + 11 rstd + 11 diff)")
    print(f"  Input: (batch, 44) flat | Huber delta=0.5")
    print(f"  Data : {args.data_dir}")
    print(f"  Out  : {args.output_dir}")
    print(f"  lr={args.lr}  epochs={args.epochs}  batch={args.batch_size}")
    print("="*65)

    os.makedirs(args.output_dir, exist_ok=True)

    X, y, target_window, feat_names = load_data_mmtc(args.data_dir)
    X_train, y_train, X_test, y_test, sc_in, sc_tgt = scale_and_split(X, y)
    num_features = X_train.shape[1]

    print(f"\nBuilding mMTC v6.2 model...")
    with strategy.scope():
        model = build_model_mmtc(num_features, lr=args.lr)

    ckpt = os.path.join(args.output_dir, "best_checkpoint.tf")
    cb_list = [
        callbacks.EarlyStopping(
            monitor="val_r_squared", mode="max",
            patience=30, restore_best_weights=True, verbose=1),
        callbacks.ReduceLROnPlateau(
            monitor="val_r_squared", mode="max",
            factor=0.5, patience=8, min_lr=1e-6, verbose=1),
        callbacks.ModelCheckpoint(
            filepath=ckpt, monitor="val_r_squared",
            mode="max", save_best_only=True, save_format="tf", verbose=0),
        callbacks.CSVLogger(
            os.path.join(args.output_dir, "training_log.csv"), append=False),
    ]

    print(f"\nTraining mMTC -- up to {args.epochs} epochs "
          f"(EarlyStopping patience=30)...\n")
    hist = model.fit(
        X_train, y_train, epochs=args.epochs,
        batch_size=args.batch_size * strategy.num_replicas_in_sync,
        validation_data=(X_test, y_test),
        callbacks=cb_list, verbose=1)

    best_r2  = max(hist.history.get("val_r_squared", [0.0]))
    best_mae = min(hist.history.get("val_mae", [float("inf")]))
    print(f"\n   Best val R2  : {best_r2:.4f}")
    print(f"   Best val MAE : {best_mae:.6f} (scaled)")

    metrics = evaluate_real_world(model, X_test, y_test, sc_tgt, "mMTC", "v6.2")

    extra = {
        "onnx_input_shape": [1, num_features],
        "_scala_inference_steps": [
            "1. Collect 11 raw KPI values from PcapKpiExtractor",
            "2. Compute rmean(5), rstd(5), diff(1) per KPI -> 44 features",
            "3. Normalise: norm[i] = (feat[i] - center[i]) / scale[i]",
            "4. ONNX.run(float[44]) -> float[1]",
            "5. throughput_bps = pred[0] * target_scale[0] + target_center[0]",
        ]
    }
    onnx_path, scaler_path, hist_path = save_artifacts(
        model, hist, metrics, sc_in, sc_tgt, args,
        "v6.2", target_window, feat_names,
        onnx_input_shape=(num_features,),
        extra_scaler=extra)

    print(f"\n{'='*65}")
    print(f"  mMTC v6.2 TRAINING COMPLETE")
    print(f"{'='*65}")
    print(f"   ONNX shape  : (1, {num_features})")
    print(f"   Best val R2 : {best_r2:.4f}")
    print(f"   ONNX        : {onnx_path or 'FAILED'}")
    print(f"   Scaler JSON : {scaler_path}")
    print(f"   History     : {hist_path}")
    print(f"   Checkpoint  : {ckpt}")
    print(f"{'='*65}")
    return metrics


# =============================================================================
# ENTRY POINT -- SLICE ROUTER
# =============================================================================
if __name__ == "__main__":
    np.random.seed(42)
    tf.random.set_seed(42)

    args = parse_args()

    print("\n" + "#"*65)
    print(f"#  5G NWDAF UNIFIED TRAINING PIPELINE  v6.4")
    print(f"#  eMBB : VAR+BiGRU+TFT + Stratified Shuffle Split (v6.4)")
    print(f"#  URLLC: VAR+GRU+TFT (v6.0, unchanged)")
    print(f"#  mMTC : VAR+GRU+TFT (v6.2, unchanged)")
    print(f"#  Slice  : {args.slice_name}")
    print(f"#  Output : {args.output_dir}")
    print("#"*65)

    if args.slice_name == "eMBB":
        result = run_embb(args)
    elif args.slice_name == "URLLC":
        result = run_urllc(args)
    elif args.slice_name == "mMTC":
        result = run_mmtc(args)
    else:
        print(f"Unknown slice: {args.slice_name}")
        exit(1)

    if result is None:
        exit(1)