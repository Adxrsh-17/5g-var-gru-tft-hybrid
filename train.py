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
# CONFIG
# =====================================================
CONFIG = {
    "window": 120,          # Lookback window (e.g., 120 seconds)
    "epochs": 60,
    "batch_size": 64,
    "lr": 0.0005,           # Lower LR for stability
    "var_lags": 5,
    "slice_filter": "Naver" # CHANGE THIS to: 'mMTC', 'Naver', or 'Youtube'
}

# =====================================================
# FEATURE SELECTION (The "Worthy" 7)
# =====================================================
# We map the raw 36 columns to 7 independent dimensions
# to capture the full state of the network slice.
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
# LOAD AND MAP DATA (CSV Support + Slice Filtering)
# =====================================================
def load_and_map_data(data_path, slice_name=None):
    """
    Load dataset (CSV/Parquet), filter by Slice, and map independent features.
    """
    print(f"🔍 Searching for files in: {data_path}")
    
    # Support both CSV (Scala output) and Parquet (Consumer output)
    files = glob.glob(os.path.join(data_path, "**", "*.csv"), recursive=True)
    if not files:
        files = glob.glob(os.path.join(data_path, "**", "*.parquet"), recursive=True)
        
    if not files:
        raise FileNotFoundError(f"No CSV or Parquet files found in {data_path}.")
    
    print(f"📂 Found {len(files)} file(s). Loading...")
    
    dfs = []
    for f in files:
        try:
            if f.endswith('.parquet'):
                df = pd.read_parquet(f)
            else:
                df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Could not read {f}: {e}")
            
    if not dfs: raise ValueError("No data loaded.")

    raw_data = pd.concat(dfs, ignore_index=True)
    
    # --- CRITICAL: FILTER BY SLICE TYPE ---
    # Time series models fail if you mix different slices (e.g., IoT and Video) 
    # into one continuous line.
    if 'Slice_Type' in raw_data.columns and slice_name:
        print(f"✂️ Filtering for Slice_Type: {slice_name}")
        raw_data = raw_data[raw_data['Slice_Type'].astype(str).str.contains(slice_name, case=False, na=False)]
    
    # Ensure time sorting (Serial_No from Scala script)
    if 'Serial_No' in raw_data.columns:
        raw_data = raw_data.sort_values('Serial_No')
        
    print(f"✅ Loaded {len(raw_data)} samples for training.")
    
    # Map to the 7 Independent Features
    mapped_data = pd.DataFrame()
    for target, source in FEATURE_MAP.items():
        if source not in raw_data.columns:
            print(f"⚠️ Warning: {source} not found. Filling zeros.")
            mapped_data[target] = 0
        else:
            col_data = raw_data[source].copy()
            # Log transform huge volume metrics to stabilize Neural Net
            if target in ['throughput', 'packets']:
                mapped_data[target] = np.log1p(col_data)
            else:
                mapped_data[target] = col_data

    # Clean data (New Pandas syntax)
    mapped_data = mapped_data.ffill().bfill().fillna(0)
    
    print(f"\n📊 Final Independent Features: {list(mapped_data.columns)}")
    return mapped_data

# =====================================================
# TFT-STYLE COMPONENTS
# =====================================================
class GatedResidualNetwork(layers.Layer):
    """Gated Residual Network from Temporal Fusion Transformer"""
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
    """Multi-head attention for temporal patterns"""
    def __init__(self, d_model, num_heads=4, dropout=0.1):
        super().__init__()
        self.mha = layers.MultiHeadAttention(num_heads, d_model // num_heads)
        self.norm = layers.LayerNormalization()
        self.dropout = layers.Dropout(dropout)
        
    def call(self, x, training=False):
        attn_out = self.mha(x, x)
        attn_out = self.dropout(attn_out, training=training)
        return self.norm(x + attn_out)

# =====================================================
# VAR-GRU-TFT HYBRID MODEL
# =====================================================
def build_var_gru_tft_model(input_shape):
    inputs = layers.Input(shape=input_shape)
    
    # Feature transformation
    x = GatedResidualNetwork(128, dropout=0.15)(inputs)
    
    # Temporal encoding (GRU)
    x = layers.GRU(256, return_sequences=True, dropout=0.2)(x)
    x = layers.GRU(128, return_sequences=True, dropout=0.2)(x)
    
    # Temporal Attention
    x = TemporalAttention(128, num_heads=4, dropout=0.1)(x)
    
    # Global Context
    x = layers.GRU(64, dropout=0.2)(x)
    x = GatedResidualNetwork(64, dropout=0.1)(x)
    
    # Output
    outputs = layers.Dense(input_shape[-1])(x)
    
    model = models.Model(inputs, outputs)
    model.compile(optimizer=optimizers.Adam(CONFIG["lr"]), loss='mse', metrics=['mae'])
    return model

# =====================================================
# SEQUENCE CREATION
# =====================================================
def create_sequences(data, window):
    X, Y = [], []
    for i in range(len(data) - window - 1):
        X.append(data[i:i + window])
        Y.append(data[i + window])
    return np.array(X), np.array(Y)

# =====================================================
# TRAINING PIPELINE
# =====================================================
def train_forecasting_model(data_path):
    # 1. Load Data
    print("\n" + "="*60 + "\nSTEP 1: Loading Independent Data\n" + "="*60)
    df = load_and_map_data(data_path, slice_name=CONFIG['slice_filter'])
    
    if len(df) < CONFIG['window'] * 2:
        raise ValueError(f"Not enough data for slice {CONFIG['slice_filter']}. Try a different slice.")

    # 2. Split
    n = len(df)
    train_idx = int(0.70 * n)
    val_idx = int(0.85 * n)
    
    train_data = df.iloc[:train_idx].values
    val_data = df.iloc[train_idx:val_idx].values
    test_data = df.iloc[val_idx:].values
    
    # 3. Scale
    scaler = RobustScaler()
    train_scaled = scaler.fit_transform(train_data)
    val_scaled = scaler.transform(val_data)
    test_scaled = scaler.transform(test_data)
    
    # 4. VAR Model
    print("\n" + "="*60 + "\nSTEP 3: Fitting VAR (Linear Baseline)\n" + "="*60)
    train_df_var = pd.DataFrame(train_scaled, columns=TARGET_FEATURES)
    try:
        var_model = VAR(train_df_var).fit(maxlags=CONFIG["var_lags"])
        k = var_model.k_ar
        print(f"✅ VAR fitted. Lag order: {k}")
    except Exception as e:
        print(f"⚠️ VAR failed: {e}. Using zero-residuals.")
        var_model = None
        k = 0

    # 5. Residuals
    if var_model:
        # Forecast only works if we have k history points
        var_val = var_model.forecast(train_scaled[-k:], len(val_scaled))
        var_test = var_model.forecast(np.vstack([train_scaled[-k:], val_scaled]), len(test_scaled))
        
        residual_train = train_scaled[k:] - var_model.fittedvalues.values
        residual_val = val_scaled - var_val
        residual_test = test_scaled - var_test
    else:
        residual_train, residual_val, residual_test = train_scaled, val_scaled, test_scaled
        var_test = np.zeros_like(test_scaled)

    # 6. Sequences
    X_train, Y_train = create_sequences(residual_train, CONFIG["window"])
    X_val, Y_val = create_sequences(residual_val, CONFIG["window"])
    X_test, Y_test = create_sequences(residual_test, CONFIG["window"])
    
    # 7. Model
    print("\n" + "="*60 + "\nSTEP 5: Training VAR-GRU-TFT\n" + "="*60)
    model = build_var_gru_tft_model(input_shape=(X_train.shape[1], X_train.shape[2]))
    
    history = model.fit(
        X_train, Y_train,
        validation_data=(X_val, Y_val),
        epochs=CONFIG["epochs"],
        batch_size=CONFIG["batch_size"],
        callbacks=[
            callbacks.EarlyStopping(patience=10, restore_best_weights=True),
            callbacks.ReduceLROnPlateau(patience=5, factor=0.5)
        ],
        verbose=1
    )
    
    # 8. Evaluation & Reconstruction
    print("\n" + "="*60 + "\nSTEP 7: Evaluation (Inverse Log Transform)\n" + "="*60)
    residual_pred = model.predict(X_test)
    
    # Match lengths
    L = min(len(residual_pred), len(var_test) - CONFIG["window"])
    var_comp = var_test[CONFIG["window"]:CONFIG["window"] + L]
    pred_comp = residual_pred[:L]
    
    final_pred_scaled = var_comp + pred_comp
    
    # Inverse Scaling
    y_pred = scaler.inverse_transform(final_pred_scaled)
    y_true = scaler.inverse_transform(test_scaled[CONFIG["window"]:CONFIG["window"] + L])
    
    # Inverse Log Transform (Expm1) for Volume Metrics
    # Indices: 0=throughput, 1=packets
    y_pred[:, 0] = np.expm1(y_pred[:, 0]) # Throughput
    y_true[:, 0] = np.expm1(y_true[:, 0])
    y_pred[:, 1] = np.expm1(y_pred[:, 1]) # Packets
    y_true[:, 1] = np.expm1(y_true[:, 1])
    
    # Metrics
    for i, feature in enumerate(TARGET_FEATURES):
        rmse = np.sqrt(mean_squared_error(y_true[:, i], y_pred[:, i]))
        mae = mean_absolute_error(y_true[:, i], y_pred[:, i])
        print(f"{feature:15s} | RMSE: {rmse:10.2f} | MAE: {mae:10.2f}")
        
    return model

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    # Path to the folder containing final40kpi.csv
    possible_paths = [
        "/kaggle/input/final40kpi", 
        "/kaggle/input/5g-36kpi",
        "/opt/spark/work-dir", # Spark output path
        "."
    ]
    DATA_PATH = next((p for p in possible_paths if os.path.exists(p)), ".")
    print(f"📍 Data Path: {DATA_PATH}")

    try:
        model = train_forecasting_model(DATA_PATH)
        print("\n✅ Process Complete.")
        model.save("var_gru_tft_final.h5")
    except Exception as e:
        print(f"\n❌ Error: {e}")
