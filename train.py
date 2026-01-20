# =====================================================
# VAR-GRU-TFT Hybrid Model for 5G Slice Forecasting
# (Optimized for 36-KPI Independent Dataset)
# =====================================================

import os
import glob
import warnings
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, models, callbacks, optimizers
from statsmodels.tsa.api import VAR
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error

# Kaggle-specific warnings suppression
warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

# Reproducibility
np.random.seed(42)
tf.random.set_seed(42)

# GPU optimization check
print(f"TensorFlow Version: {tf.__version__}")
if tf.config.list_physical_devices("GPU"):
    print("🚀 GPU detected and enabled")
else:
    print("⚠️ No GPU detected. Training might be slow.")

# =====================================================
# CONFIGURATION
# =====================================================
CONFIG = {
    "window": 120,          # Lookback window (e.g., 120 seconds)
    "epochs": 60,
    "batch_size": 64,
    "lr": 0.0005,           # Lower LR for stability
    "var_lags": 5,
    # SELECT YOUR SLICE HERE: 'Naver', 'Youtube', or 'MMTC'
    "target_slice": "Naver" 
}

# Map user-friendly names to the actual dataset labels from your Scala script
SLICE_LABEL_MAP = {
    "Naver": "eMBB",
    "Youtube": "URLLC",
    "MMTC": "mMTC"
}

# =====================================================
# FEATURE SELECTION (The "Worthy" 7)
# =====================================================
# We map the raw 36 columns to 7 independent dimensions
FEATURE_MAP = {
    "throughput":   "Throughput_bps",       # [Target] Volume
    "packets":      "Total_Packets",        # [Target] Activity
    "jitter":       "Jitter",               # [Quality] Stability Proxy
    "latency":      "Avg_IAT",              # [Quality] Latency Proxy
    "reliability":  "Retransmission_Ratio", # [Health] Loss Indicator
    "congestion":   "Avg_Win_Size",         # [Health] Capacity/Congestion
    "complexity":   "Entropy_Score"         # [Context] Traffic Type
}

TARGET_FEATURES = list(FEATURE_MAP.keys())

# =====================================================
# DATA LOADING & ENGINEERING
# =====================================================
def load_and_map_data(data_path, target_slice_name):
    """
    Load dataset (CSV/Parquet), filter by correct Slice Label, and map features.
    """
    print(f"🔍 Searching for files in: {data_path}")
    
    # 1. Recursive Search for Parquet or CSV
    files = glob.glob(os.path.join(data_path, "**", "*.parquet"), recursive=True)
    if not files:
        files = glob.glob(os.path.join(data_path, "**", "*.csv"), recursive=True)
        
    if not files:
        raise FileNotFoundError(f"❌ No files found in {data_path}. Check your dataset connection.")
    
    print(f"📂 Found {len(files)} file(s). Loading...")
    
    # 2. Load all parts
    dfs = []
    for f in files:
        try:
            if f.endswith('.parquet'):
                df = pd.read_parquet(f)
            else:
                df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Skipping {os.path.basename(f)}: {e}")
            
    if not dfs: raise ValueError("No valid data loaded.")

    raw_data = pd.concat(dfs, ignore_index=True)
    
    # 3. Filter by Slice Type (Fixing the Naver vs eMBB issue)
    # Get the actual dataset label (e.g., 'eMBB') from our map
    dataset_label = SLICE_LABEL_MAP.get(target_slice_name, target_slice_name)
    
    print(f"✂️ Filtering for Slice Label: '{dataset_label}' (User selected: '{target_slice_name}')")
    
    # Check if column exists
    if 'Slice_Type' not in raw_data.columns:
        # Fallback for old CSVs
        if 'slice_label' in raw_data.columns:
            raw_data.rename(columns={'slice_label': 'Slice_Type'}, inplace=True)
        else:
            print(f"⚠️ 'Slice_Type' column missing. Available: {raw_data.columns}")
            print("   Assuming single-slice dataset.")
            raw_data['Slice_Type'] = dataset_label

    # Apply Filter
    filtered_data = raw_data[raw_data['Slice_Type'].astype(str) == dataset_label].copy()
    
    if len(filtered_data) == 0:
        print(f"❌ ERROR: No data found for label '{dataset_label}'.")
        print(f"   Available labels in dataset: {raw_data['Slice_Type'].unique()}")
        raise ValueError("Data filter returned empty result.")

    # 4. Sort by Time
    if 'Serial_No' in filtered_data.columns:
        filtered_data = filtered_data.sort_values('Serial_No')
    else:
        # Fallback if Serial_No is missing but we have 40kpi output
        print("⚠️ 'Serial_No' missing, assuming data is already time-ordered.")
        
    print(f"✅ Loaded {len(filtered_data)} samples for training.")
    
    # 5. Map Features & Log Transform
    mapped_data = pd.DataFrame()
    for target, source in FEATURE_MAP.items():
        if source not in filtered_data.columns:
            print(f"⚠️ Warning: Column '{source}' not found. Filling with zeros.")
            mapped_data[target] = 0
        else:
            col_data = filtered_data[source].copy()
            # Log transform heavy-tailed metrics to stabilize gradients
            if target in ['throughput', 'packets']:
                mapped_data[target] = np.log1p(col_data)
            else:
                mapped_data[target] = col_data

    # 6. Final Cleanup
    mapped_data = mapped_data.ffill().bfill().fillna(0)
    
    print(f"📊 Final Feature Matrix: {mapped_data.shape}")
    return mapped_data

# =====================================================
# MODEL ARCHITECTURE (TFT Components)
# =====================================================
class GatedResidualNetwork(layers.Layer):
    """GRN: The core building block of TFT"""
    def __init__(self, units, dropout=0.1):
        super().__init__()
        self.units = units
        self.dense1 = layers.Dense(units, activation='elu')
        self.dense2 = layers.Dense(units)
        self.dropout = layers.Dropout(dropout)
        self.gate = layers.Dense(units, activation='sigmoid')
        self.norm = layers.LayerNormalization()
        
    def build(self, input_shape):
        if input_shape[-1] != self.units:
            self.skip_layer = layers.Dense(self.units)
        else:
            self.skip_layer = None
        
    def call(self, x, training=False):
        residual = x
        x = self.dense1(x)
        x = self.dropout(x, training=training)
        x = self.dense2(x)
        gate = self.gate(x)
        x = x * gate
        if self.skip_layer is not None:
            residual = self.skip_layer(residual)
        return self.norm(x + residual)

class TemporalAttention(layers.Layer):
    """Interpretable Multi-Head Attention"""
    def __init__(self, d_model, num_heads=4, dropout=0.1):
        super().__init__()
        self.mha = layers.MultiHeadAttention(num_heads, d_model // num_heads)
        self.norm = layers.LayerNormalization()
        self.dropout = layers.Dropout(dropout)
        
    def call(self, x, training=False):
        attn_out = self.mha(x, x)
        attn_out = self.dropout(attn_out, training=training)
        return self.norm(x + attn_out)

def build_var_gru_tft_model(input_shape):
    """Constructs the Hybrid Model"""
    inputs = layers.Input(shape=input_shape)
    
    # 1. Feature Projection (GRN)
    x = GatedResidualNetwork(128, dropout=0.15)(inputs)
    
    # 2. Sequential Encoder (GRU Stack)
    x = layers.GRU(256, return_sequences=True, dropout=0.2)(x)
    x = layers.GRU(128, return_sequences=True, dropout=0.2)(x)
    
    # 3. Temporal Self-Attention (TFT)
    x = TemporalAttention(128, num_heads=4, dropout=0.1)(x)
    
    # 4. Global Context Decoding
    x = layers.GRU(64, dropout=0.2)(x)
    x = GatedResidualNetwork(64, dropout=0.1)(x)
    
    # 5. Output Projection
    outputs = layers.Dense(input_shape[-1])(x)
    
    model = models.Model(inputs, outputs, name="VAR_GRU_TFT")
    model.compile(optimizer=optimizers.Adam(CONFIG["lr"]), loss='mse', metrics=['mae'])
    return model

# =====================================================
# PIPELINE HELPERS
# =====================================================
def create_sequences(data, window):
    X, Y = [], []
    for i in range(len(data) - window - 1):
        X.append(data[i:i + window])
        Y.append(data[i + window])
    return np.array(X), np.array(Y)

def train_forecasting_model(data_path):
    # --- Step 1: Data Loading ---
    print("\n" + "="*60 + "\nSTEP 1: Loading & Mapping Data\n" + "="*60)
    df = load_and_map_data(data_path, CONFIG['target_slice'])
    
    # Validation check
    if len(df) < CONFIG['window'] * 5:
        raise ValueError(f"Dataset too small ({len(df)} rows). Need at least {CONFIG['window']*5}.")

    # --- Step 2: Splitting ---
    n = len(df)
    train_idx = int(0.70 * n)
    val_idx = int(0.85 * n)
    
    train_data = df.iloc[:train_idx].values
    val_data = df.iloc[train_idx:val_idx].values
    test_data = df.iloc[val_idx:].values
    
    # --- Step 3: Robust Scaling ---
    print("\nSTEP 2: Scaling Data")
    scaler = RobustScaler()
    train_scaled = scaler.fit_transform(train_data)
    val_scaled = scaler.transform(val_data)
    test_scaled = scaler.transform(test_data)
    
    # --- Step 4: VAR (Linear Baseline) ---
    print("\nSTEP 3: Fitting VAR Model")
    train_df_var = pd.DataFrame(train_scaled, columns=TARGET_FEATURES)
    try:
        var_model = VAR(train_df_var).fit(maxlags=CONFIG["var_lags"])
        k = var_model.k_ar
        print(f"✅ VAR fitted. Lag order: {k}")
    except Exception as e:
        print(f"⚠️ VAR failed: {e}. Using zero-residuals (Pure GRU-TFT mode).")
        var_model = None
        k = 0

    # Calculate Residuals (Remove linear component)
    if var_model:
        var_val_pred = var_model.forecast(train_scaled[-k:], len(val_scaled))
        var_test_pred = var_model.forecast(np.vstack([train_scaled[-k:], val_scaled]), len(test_scaled))
        
        residual_train = train_scaled[k:] - var_model.fittedvalues.values
        residual_val = val_scaled - var_val_pred
        residual_test = test_scaled - var_test_pred
    else:
        residual_train, residual_val, residual_test = train_scaled, val_scaled, test_scaled
        var_test_pred = np.zeros_like(test_scaled)

    # --- Step 5: Sequence Generation ---
    X_train, Y_train = create_sequences(residual_train, CONFIG["window"])
    X_val, Y_val = create_sequences(residual_val, CONFIG["window"])
    X_test, Y_test = create_sequences(residual_test, CONFIG["window"])
    
    print(f"📦 Sequences created. Train: {X_train.shape}, Test: {X_test.shape}")

    # --- Step 6: Model Training ---
    print("\nSTEP 4: Training Hybrid Model")
    model = build_var_gru_tft_model(input_shape=(X_train.shape[1], X_train.shape[2]))
    
    history = model.fit(
        X_train, Y_train,
        validation_data=(X_val, Y_val),
        epochs=CONFIG["epochs"],
        batch_size=CONFIG["batch_size"],
        callbacks=[
            callbacks.EarlyStopping(patience=10, restore_best_weights=True, verbose=1),
            callbacks.ReduceLROnPlateau(patience=5, factor=0.5, verbose=1)
        ],
        verbose=1
    )
    
    # --- Step 7: Evaluation & Inversion ---
    print("\n" + "="*60 + "\nSTEP 5: Evaluation\n" + "="*60)
    
    # Predict Residuals
    resid_pred = model.predict(X_test, verbose=0)
    
    # Add back Linear Component (VAR)
    # Align lengths (Sequence generation trims the start)
    L = len(resid_pred)
    # The VAR forecast aligned with the test residuals
    var_comp = var_test_pred[CONFIG["window"]:CONFIG["window"]+L]
    
    # Final Scaled Prediction
    final_pred_scaled = var_comp + resid_pred
    
    # Inverse Scale
    y_pred = scaler.inverse_transform(final_pred_scaled)
    y_true = scaler.inverse_transform(test_scaled[CONFIG["window"]:CONFIG["window"]+L])
    
    # Inverse Log Transform (np.expm1) ONLY for Volume metrics
    # Indices 0 (throughput) and 1 (packets) are log-transformed
    for i in [0, 1]:
        y_pred[:, i] = np.expm1(y_pred[:, i])
        y_true[:, i] = np.expm1(y_true[:, i])
        # Clip negative predictions for physical realism
        y_pred[:, i] = np.maximum(y_pred[:, i], 0)

    # Calculate Metrics
    print(f"{'FEATURE':<15} | {'RMSE':<10} | {'MAE':<10} | {'MAPE':<10}")
    print("-" * 55)
    
    for i, feature in enumerate(TARGET_FEATURES):
        rmse = np.sqrt(mean_squared_error(y_true[:, i], y_pred[:, i]))
        mae = mean_absolute_error(y_true[:, i], y_pred[:, i])
        
        # Safe MAPE
        non_zero = y_true[:, i] > 1e-6
        if np.sum(non_zero) > 0:
            mape = mean_absolute_percentage_error(y_true[non_zero, i], y_pred[non_zero, i]) * 100
        else:
            mape = 0.0
            
        print(f"{feature:<15} | {rmse:<10.2f} | {mae:<10.2f} | {mape:<9.2f}%")
        
    return model

# =====================================================
# MAIN ENTRY POINT
# =====================================================
if __name__ == "__main__":
    # 1. Find Data Path
    # Helper to find where Kaggle mounted the dataset
    search_roots = ["/kaggle/input", "."]
    DATA_PATH = None
    
    # Look for the '5g-36kpi' folder or similar
    for root in search_roots:
        candidates = glob.glob(os.path.join(root, "*5g*"))
        if candidates:
            DATA_PATH = candidates[0]
            break
            
    if not DATA_PATH:
        # Fallback if specific folder name varies
        DATA_PATH = "/kaggle/input" 
        
    print(f"📍 Detected Data Path: {DATA_PATH}")

    # 2. Run
    try:
        model = train_forecasting_model(DATA_PATH)
        print("\n✅ Training Complete. Saving Model...")
        model.save("var_gru_tft_final.h5")
        print("💾 Model saved.")
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
