import os
import glob
import warnings
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, models, callbacks, optimizers, losses
from statsmodels.tsa.api import VAR
from sklearn.preprocessing import RobustScaler, MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error

# Suppress warnings
warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

# Reproducibility
np.random.seed(42)
tf.random.set_seed(42)

# =====================================================
# ⚡ PARALLEL STRATEGY
# =====================================================
try:
    strategy = tf.distribute.MirroredStrategy()
    print(f"🚀 ACCELERATION: Running on {strategy.num_replicas_in_sync} GPU(s)")
except:
    strategy = tf.distribute.get_strategy()
    print("⚠️ ACCELERATION: Single Device Mode")

# =====================================================
# ⚙️ CONFIGURATION (TUNED)
# =====================================================
CONFIG = {
    "window": 60,            # Reduced window (120 is too long for unstable bursts)
    "forecast_horizon": 1,
    "epochs": 150,           # Increased epochs
    "batch_size": 256,       # Larger batch for stable gradients
    "lr": 0.0001,            # Slower learning rate to prevent divergence
    "var_lags": 3,           # Reduced lags to avoid overfitting linear noise
    "target_slice": "Naver"  # eMBB
}

SLICE_MAP = {"Naver": "eMBB", "Youtube": "URLLC", "MMTC": "mMTC"}

# =====================================================
# 🧠 FEATURE ENGINEERING
# =====================================================
FEATURE_MAP = {
    "throughput":   "Throughput_bps",
    "packets":      "Total_Packets",
    "jitter":       "Jitter",
    "latency":      "Avg_IAT",
    "reliability":  "Retransmission_Ratio",
    "congestion":   "Avg_Win_Size",
    "complexity":   "Entropy_Score"
}
TARGET_FEATURES = list(FEATURE_MAP.keys())

# =====================================================
# 📂 DATA LOADING
# =====================================================
def load_and_prep_data(data_path, slice_name):
    print(f"🔍 Scanning: {data_path}")
    files = glob.glob(os.path.join(data_path, "**", "*.parquet"), recursive=True)
    if not files: files = glob.glob(os.path.join(data_path, "**", "*.csv"), recursive=True)
    if not files: raise FileNotFoundError("❌ No dataset files found!")

    dfs = []
    for f in files:
        try:
            if f.endswith('.parquet'): df = pd.read_parquet(f)
            else: df = pd.read_csv(f)
            dfs.append(df)
        except: pass
        
    raw_data = pd.concat(dfs, ignore_index=True)
    
    # Slice Filter
    target_label = SLICE_MAP.get(slice_name, slice_name)
    print(f"✂️ Filtering for Slice Label: '{target_label}'")
    
    if 'Slice_Type' not in raw_data.columns:
        if 'slice_label' in raw_data.columns:
            raw_data.rename(columns={'slice_label': 'Slice_Type'}, inplace=True)
        else:
            raw_data['Slice_Type'] = target_label

    df_slice = raw_data[raw_data['Slice_Type'].astype(str) == target_label].copy()
    
    if len(df_slice) == 0:
        raise ValueError(f"❌ No data for '{target_label}'.")
        
    if 'Serial_No' in df_slice.columns:
        df_slice = df_slice.sort_values('Serial_No')
        
    print(f"✅ Loaded {len(df_slice)} samples.")

    # Map Features (NO LOG TRANSFORM - Using RobustScaler only)
    final_df = pd.DataFrame()
    for target, source in FEATURE_MAP.items():
        if source in df_slice.columns:
            final_df[target] = df_slice[source].copy()
        else:
            final_df[target] = 0.0
            
    return final_df.ffill().bfill().fillna(0)

# =====================================================
# 🏗️ MODEL ARCHITECTURE (Huber Loss + GRU-TFT)
# =====================================================
class GatedResidualNetwork(layers.Layer):
    def __init__(self, units, dropout):
        super().__init__()
        self.units = units
        self.elu_dense = layers.Dense(units, activation='elu')
        self.linear_dense = layers.Dense(units)
        self.dropout = layers.Dropout(dropout)
        self.gate = layers.Dense(units, activation='sigmoid')
        self.norm = layers.LayerNormalization()

    def call(self, x):
        skip = x if x.shape[-1] == self.units else layers.Dense(self.units)(x)
        x = self.elu_dense(x)
        x = self.dropout(x)
        x = self.linear_dense(x)
        x = x * self.gate(x)
        return self.norm(skip + x)

def build_model(input_shape):
    inputs = layers.Input(shape=input_shape)
    
    # Feature Extraction
    x = GatedResidualNetwork(64, 0.1)(inputs) # Reduced units to prevent overfitting
    
    # Sequential Modeling
    x = layers.GRU(128, return_sequences=True, dropout=0.2)(x)
    x = layers.GRU(64, return_sequences=True, dropout=0.2)(x)
    
    # Temporal Attention
    x = layers.MultiHeadAttention(num_heads=4, key_dim=32)(x, x)
    x = layers.LayerNormalization()(x)
    
    # Global Pooling
    x = layers.GlobalAveragePooling1D()(x)
    x = GatedResidualNetwork(32, 0.1)(x)
    
    outputs = layers.Dense(input_shape[-1])(x)
    
    model = models.Model(inputs, outputs, name="Stabilized_Hybrid_Model")
    
    # HUBER LOSS: Key fix for bursty traffic (ignores massive outliers)
    model.compile(optimizer=optimizers.Adam(CONFIG['lr']), 
                  loss=losses.Huber(delta=1.0), 
                  metrics=['mae'])
    return model

# =====================================================
# 🚀 MAIN PIPELINE
# =====================================================
def run_pipeline():
    # 1. Path Detection
    paths = ["/kaggle/input", ".", "/opt/spark/work-dir"]
    data_path = next((p for p in paths if os.path.exists(p)), None)
    
    # 2. Data Loading
    df = load_and_prep_data(data_path, CONFIG['target_slice'])
    
    # 3. Splits
    n = len(df)
    train_df = df.iloc[:int(0.7*n)]
    val_df = df.iloc[int(0.7*n):int(0.85*n)]
    test_df = df.iloc[int(0.85*n):]
    
    # 4. Scaling (RobustScaler handles the bursts)
    scaler = RobustScaler()
    train_scaled = scaler.fit_transform(train_df)
    val_scaled = scaler.transform(val_df)
    test_scaled = scaler.transform(test_df)
    
    # 5. VAR Baseline
    print("📈 Fitting VAR...")
    try:
        var_model = VAR(train_scaled).fit(maxlags=CONFIG['var_lags'])
        lag = var_model.k_ar
        
        def get_residuals(data, prev_data):
            if len(prev_data) < lag: return data
            hist = np.vstack([prev_data[-lag:], data])
            pred = []
            for i in range(lag, len(hist)):
                pred.append(var_model.forecast(hist[i-lag:i], 1)[0])
            return data - np.array(pred), np.array(pred)

        res_train, _ = get_residuals(train_scaled[lag:], train_scaled[:lag])
        res_val, _ = get_residuals(val_scaled, train_scaled)
        res_test, var_pred_test = get_residuals(test_scaled, val_scaled)
        print("✅ VAR fitted successfully.")
        
    except:
        print("⚠️ VAR Failed/Skipped. Using Raw Data mode.")
        res_train, res_val, res_test = train_scaled, val_scaled, test_scaled
        var_pred_test = np.zeros_like(test_scaled)

    # 6. Sequence Gen
    def make_seq(data, window):
        X, y = [], []
        for i in range(len(data)-window):
            X.append(data[i:i+window])
            y.append(data[i+window])
        return np.array(X), np.array(y)

    X_train, y_train = make_seq(res_train, CONFIG['window'])
    X_val, y_val = make_seq(res_val, CONFIG['window'])
    X_test, y_test = make_seq(res_test, CONFIG['window'])
    
    # 7. Training
    print("🔥 Starting Training (Huber Loss)...")
    with strategy.scope():
        model = build_model((CONFIG['window'], 7))
    
    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=CONFIG['epochs'],
        batch_size=CONFIG['batch_size'] * strategy.num_replicas_in_sync,
        callbacks=[
            callbacks.EarlyStopping(patience=20, restore_best_weights=True, monitor='val_mae'),
            callbacks.ReduceLROnPlateau(patience=5, factor=0.5, min_lr=1e-6)
        ],
        verbose=1
    )
    
    # 8. Evaluation
    print("📊 Evaluating...")
    resid_pred = model.predict(X_test, batch_size=CONFIG['batch_size'])
    
    L = len(resid_pred)
    final_pred = var_pred_test[CONFIG['window']:CONFIG['window']+L] + resid_pred
    
    # Inverse Scale
    y_pred_real = scaler.inverse_transform(final_pred)
    y_true_real = scaler.inverse_transform(test_scaled[CONFIG['window']:CONFIG['window']+L])
    
    # Force Positive (Physics constraint)
    y_pred_real = np.maximum(y_pred_real, 0)

    # Metrics
    print(f"\n{'FEATURE':<15} | {'RMSE':<12} | {'MAE':<12}")
    print("-" * 45)
    for i, name in enumerate(TARGET_FEATURES):
        rmse = np.sqrt(mean_squared_error(y_true_real[:, i], y_pred_real[:, i]))
        mae = mean_absolute_error(y_true_real[:, i], y_pred_real[:, i])
        print(f"{name:<15} | {rmse:<12.4f} | {mae:<12.4f}")
        
    model.save("Stabilized_Hybrid_Model.h5")
    print("\n✅ Saved: Stabilized_Hybrid_Model.h5")

if __name__ == "__main__":
    run_pipeline()
