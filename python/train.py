# =====================================================
# 5G HYBRID MODEL TRAINING (Cluster & Scala Ready)
# =====================================================
# This script trains the VAR-GRU-TFT model on Spark-generated KPIs
# and exports artifacts (ONNX + JSON) for the Scala Inference Engine.

import os
import glob
import warnings
import json
import argparse
import numpy as np
import pandas as pd
import tensorflow as tf
import tf2onnx
from tensorflow.keras import layers, models, callbacks, optimizers, losses
from statsmodels.tsa.api import VAR
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error

# Suppress warnings
warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

# Reproducibility
np.random.seed(42)
tf.random.set_seed(42)

# =====================================================
# ⚡ PARALLEL STRATEGY (GPU ACCELERATION)
# =====================================================
try:
    strategy = tf.distribute.MirroredStrategy()
    print(f"🚀 ACCELERATION: Running on {strategy.num_replicas_in_sync} GPU(s)")
except:
    strategy = tf.distribute.get_strategy()
    print("⚠️ ACCELERATION: Single Device Mode (CPU)")

# =====================================================
# ⚙️ COMMAND LINE ARGUMENTS
# =====================================================
def parse_args():
    parser = argparse.ArgumentParser(description='5G KPI Slice-Wise Model Training')
    parser.add_argument('--data_dir', type=str, required=True, 
                       help='Path to input Parquet files (e.g., /tmp/5g_train/eMBB)')
    parser.add_argument('--slice_name', type=str, required=True, 
                       help='Name of the slice (eMBB, URLLC, mMTC)')
    parser.add_argument('--output_dir', type=str, default='./artifacts',
                       help='Directory to save model.onnx and scaler_params.json')
    parser.add_argument('--epochs', type=int, default=50,
                       help='Number of training epochs')
    parser.add_argument('--window', type=int, default=60,
                       help='Time window length for sequences')
    return parser.parse_args()

# =====================================================
# ⚙️ CONFIGURATION
# =====================================================
def get_config(args):
    return {
        "window": args.window,
        "forecast_horizon": 1,
        "epochs": args.epochs,
        "batch_size": 256,
        "lr": 0.0001,
        "var_lags": 3,
        "target_slice": args.slice_name
    }

# =====================================================
# 🧠 FEATURE MAPPING (Matches PcapKpiExtractor.scala)
# =====================================================
# We select 7 robust features available in your Parquet output
FEATURE_COLUMNS = [
    "Throughput_bps",
    "Total_Packets",
    "Jitter_Variance",
    "Avg_Packet_Size",
    "Active_Flows",
    "TCP_Packets",
    "UDP_Packets"
]

# =====================================================
# 📂 DATA LOADING
# =====================================================
def load_and_prep_data(data_path, slice_name):
    if not data_path or not os.path.exists(data_path):
        raise FileNotFoundError(f"❌ Data path not found: {data_path}")

    print(f"🔍 Scanning: {data_path}")
    # recursive=True to find parquet files inside partition folders (sliceType=eMBB/...)
    files = glob.glob(os.path.join(data_path, "**", "*.parquet"), recursive=True)
    
    if not files:
        raise FileNotFoundError("❌ No .parquet files found! Run PcapKpiExtractor first.")

    print(f"   > Found {len(files)} files. Loading...")
    
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"   > Skipped bad file {f}: {e}")
            pass
        
    if not dfs: raise ValueError("❌ No valid data loaded.")
    
    raw_data = pd.concat(dfs, ignore_index=True)
    
    # Filter for specific slice if 'sliceType' column exists
    if 'sliceType' in raw_data.columns:
        print(f"✂️ Filtering for Slice: '{slice_name}'")
        df_slice = raw_data[raw_data['sliceType'] == slice_name].copy()
    else:
        print("⚠️ 'sliceType' column not found, using all data.")
        df_slice = raw_data.copy()

    if len(df_slice) == 0:
        raise ValueError(f"❌ No records found for slice '{slice_name}'")

    # Sort by time if available, else assume order
    if 'window_end' in df_slice.columns:
        df_slice = df_slice.sort_values('window_end')

    # Select and Clean Features
    final_df = df_slice[FEATURE_COLUMNS].copy()
    final_df = final_df.ffill().bfill().fillna(0)
    
    print(f"✅ Loaded {len(final_df)} samples with {len(FEATURE_COLUMNS)} features.")
    return final_df

# =====================================================
# 🏗️ MODEL ARCHITECTURE
# =====================================================
class GatedResidualNetwork(layers.Layer):
    def __init__(self, units, dropout):
        super().__init__()
        self.units = units
        self.elu_dense = layers.Dense(units, activation='elu')
        self.linear_dense = layers.Dense(units)
        self.dropout_layer = layers.Dropout(dropout)
        self.gate = layers.Dense(units, activation='sigmoid')
        self.norm = layers.LayerNormalization()
        self.skip_project = None 

    def build(self, input_shape):
        if input_shape[-1] != self.units:
            self.skip_project = layers.Dense(self.units)
        super().build(input_shape)

    def call(self, x):
        skip = self.skip_project(x) if self.skip_project is not None else x
        x_val = self.elu_dense(x)
        x_val = self.dropout_layer(x_val)
        x_val = self.linear_dense(x_val)
        x_val = x_val * self.gate(x) 
        return self.norm(skip + x_val)

def build_model(input_shape):
    inputs = layers.Input(shape=input_shape, name="input")
    
    # Feature Extraction
    x = GatedResidualNetwork(64, 0.1)(inputs)
    
    # Sequential Modeling
    x = layers.GRU(128, return_sequences=True, dropout=0.2)(x)
    x = layers.GRU(64, return_sequences=True, dropout=0.2)(x)
    
    # Temporal Attention
    x = layers.MultiHeadAttention(num_heads=4, key_dim=32)(x, x)
    x = layers.LayerNormalization()(x)
    
    # Global Pooling
    x = layers.GlobalAveragePooling1D()(x)
    x = GatedResidualNetwork(32, 0.1)(x)
    
    # Output matches number of features (Multivariate Forecasting)
    outputs = layers.Dense(input_shape[-1], name="output")(x)
    
    model = models.Model(inputs, outputs, name="Stabilized_Hybrid_Model")
    
    model.compile(optimizer=optimizers.Adam(CONFIG['lr']), 
                  loss=losses.Huber(delta=1.0), 
                  metrics=['mae'])
    return model

# =====================================================
# 🚀 MAIN PIPELINE
# =====================================================
def run_pipeline(args, config):
    # 1. Use provided data directory
    data_path = args.data_dir
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 2. Data Loading
    try:
        df = load_and_prep_data(data_path, config['target_slice'])
        print(f"📊 Loaded {len(df)} samples for slice {config['target_slice']}")
    except Exception as e:
        print(f"❌ Data loading failed for slice {config['target_slice']}: {str(e)}")
        return # Stop if data load fails

    # 3. Splits
    n = len(df)
    train_df = df.iloc[:int(0.8*n)]
    test_df = df.iloc[int(0.8*n):]
    
    # 4. Scaling (RobustScaler for outliers)
    scaler = RobustScaler()
    train_scaled = scaler.fit_transform(train_df)
    test_scaled = scaler.transform(test_df)
    
    # 5. Sequence Generation
    def make_seq(data, window):
        X, y = [], []
        for i in range(len(data)-window):
            X.append(data[i:i+window])
            y.append(data[i+window]) # Predict next step
        return np.array(X), np.array(y)

    X_train, y_train = make_seq(train_scaled, config['window'])
    X_test, y_test = make_seq(test_scaled, config['window'])
    
    if len(X_train) == 0:
        print("❌ Not enough data to create sequences. Need more history.")
        return

    # 6. Training
    print(f"🔥 Starting Training for {config['target_slice']} on {len(X_train)} samples...")
    with strategy.scope():
        # Input Shape: [window, features]
        model = build_model((config['window'], len(FEATURE_COLUMNS)))
    
    model.fit(
        X_train, y_train,
        epochs=config['epochs'],
        batch_size=config['batch_size'] * strategy.num_replicas_in_sync,
        verbose=1
    )
    
    # 7. Evaluation
    print("📊 Evaluating...")
    loss, mae = model.evaluate(X_test, y_test, verbose=0)
    print(f"   > Test MAE: {mae:.4f}")

    # =====================================================
    # 💾 EXPORT ARTIFACTS
    # =====================================================
    
    # A. Save ONNX for Scala
    print(f"🔄 Exporting ONNX model for {config['target_slice']}...")
    spec = (tf.TensorSpec((None, config['window'], len(FEATURE_COLUMNS)), tf.float32, name="input"),)
    
    model_proto, _ = tf2onnx.convert.from_keras(model, input_signature=spec, opset=13)
    onnx_path = os.path.join(args.output_dir, "model.onnx")
    with open(onnx_path, "wb") as f:
        f.write(model_proto.SerializeToString())
    print(f"   ✅ ONNX model saved: {onnx_path}")

    # B. Save Scaler Params for Scala
    print(f"🔄 Exporting scaler metadata for {config['target_slice']}...")
    scaler_params = {
        "center": scaler.center_.tolist(),
        "scale": scaler.scale_.tolist(),
        "features": FEATURE_COLUMNS,
        "slice_name": config['target_slice'],
        "window_size": config['window']
    }
    scaler_path = os.path.join(args.output_dir, "scaler_params.json")
    with open(scaler_path, "w") as f:
        json.dump(scaler_params, f, indent=2)
    print(f"   ✅ Scaler params saved: {scaler_path}")
    
    return onnx_path, scaler_path

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    config = get_config(args)
    
    print("=" * 50)
    print(f"🎯 5G KPI TRAINING - SLICE: {args.slice_name}")
    print(f"📂 Data Directory: {args.data_dir}")
    print(f"💾 Output Directory: {args.output_dir}")
    print(f"🔧 Configuration: {config}")
    print("=" * 50)
    
    # Run the training pipeline
    result = run_pipeline(args, config)
    
    if result:
        onnx_path, scaler_path = result
        print(f"\n🎉 Training completed successfully for slice {args.slice_name}!")
        print(f"📍 ONNX Model: {onnx_path}")
        print(f"📍 Scaler File: {scaler_path}")
    else:
        print(f"\n❌ Training failed for slice {args.slice_name}")
        exit(1)