# =====================================================
# VAR–GRU–ATTENTION Slice-Aware 5G Forecasting (FINAL STABLE)
# =====================================================

import os, glob, warnings
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, models, callbacks, optimizers, mixed_precision
from statsmodels.tsa.api import VAR
from statsmodels.tsa.stattools import adfuller
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error

# =====================================================
# ENV
# =====================================================
warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
np.random.seed(42)
tf.random.set_seed(42)

if tf.config.list_physical_devices("GPU"):
    mixed_precision.set_global_policy("mixed_float16")
    print("🚀 GPU + FP16 enabled")

# =====================================================
# CONFIG (paper-tuned)
# =====================================================
CONFIG = {
    "window": 120,
    "epochs": 60,
    "batch_size": 64,
    "lr": 0.0007
}

# =====================================================
# LOAD DATA (7 KPIs ONLY – REAL + DERIVED)
# =====================================================
def load_kpi_data():
    files = glob.glob("/kaggle/input/5g-kafka/part-*.snappy.parquet")
    df = pd.concat([pd.read_parquet(f) for f in files])

    df.rename(columns={
        "timestamp": "time",
        "slice_type": "slice",
        "bytes": "throughput",
        "packets": "packets"
    }, inplace=True)

    df = df.sort_values("time")

    slices = {}
    for s in ["MMTC", "Youtube", "Naver"]:
        tmp = df[df["slice"] == s][["throughput", "packets"]].copy()
        if len(tmp) < 1000:
            continue

        # ---------------- 7 KPIs ----------------
        tmp["pkt_rate"] = tmp["packets"] / (tmp["throughput"] + 1e-6)
        tmp["thr_diff"] = tmp["throughput"].diff().fillna(0)
        tmp["pkt_diff"] = tmp["packets"].diff().fillna(0)
        tmp["thr_ma10"] = tmp["throughput"].rolling(10).mean().fillna(method="bfill")
        tmp["pkt_ma10"] = tmp["packets"].rolling(10).mean().fillna(method="bfill")

        # eMBB physics → multiplicative
        if s in ["Youtube", "Naver"]:
            for c in tmp.columns:
                tmp[c] = np.log1p(tmp[c])

        tmp.replace([np.inf, -np.inf], 0, inplace=True)
        tmp.fillna(0, inplace=True)

        slices[s] = tmp
        print(f"{s}: {len(tmp)} samples | KPIs: {list(tmp.columns)}")

    return slices

# =====================================================
# ATTENTION
# =====================================================
class AttentionLayer(layers.Layer):
    def __init__(self, units):
        super().__init__()
        self.att = layers.MultiHeadAttention(4, units // 4)
        self.norm = layers.LayerNormalization()

    def call(self, x):
        return self.norm(x + self.att(x, x))

# =====================================================
# MODEL
# =====================================================
def build_model(shape):
    i = layers.Input(shape)
    x = layers.GRU(256, return_sequences=True)(i)
    x = layers.GRU(128, return_sequences=True)(x)
    x = AttentionLayer(128)(x)
    x = layers.GRU(64)(x)
    o = layers.Dense(1, dtype="float32")(x)
    m = models.Model(i, o)
    m.compile(optimizers.Adam(CONFIG["lr"]), "mse")
    return m

# =====================================================
# WINDOWING
# =====================================================
def make_seq(data):
    X, Y = [], []
    for i in range(len(data) - CONFIG["window"] - 1):
        X.append(data[i:i + CONFIG["window"]])
        Y.append(data[i + CONFIG["window"], 0])  # target = throughput
    return np.array(X), np.array(Y)

# =====================================================
# CLEAN ARRAY (CRITICAL FIX)
# =====================================================
def clean_array(x):
    return np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)

# =====================================================
# TRAIN ONE SLICE (70–15–15)
# =====================================================
def train_slice(name, df):

    print(f"\n{name} | ADF p-value:", adfuller(df["throughput"])[1])

    n = len(df)
    t1 = int(0.70 * n)
    t2 = int(0.85 * n)

    train, val, test = df[:t1], df[t1:t2], df[t2:]

    scaler = RobustScaler()
    train_s = scaler.fit_transform(train)
    val_s = scaler.transform(val)
    test_s = scaler.transform(test)

    train_df = pd.DataFrame(train_s, columns=df.columns)
    val_df = pd.DataFrame(val_s, columns=df.columns)
    test_df = pd.DataFrame(test_s, columns=df.columns)

    # ---------------- VAR
    var = VAR(train_df).fit(5)
    k = var.k_ar

    var_val = var.forecast(train_df.values[-k:], len(val_df))
    var_test = var.forecast(
        np.vstack([train_df.values[-k:], val_df.values]),
        len(test_df)
    )

    # ---------------- Residuals
    res_train = train_df.iloc[k:].values - var.fittedvalues.values
    res_val = val_df.values - var_val
    res_test = test_df.values - var_test

    # 🔥 CRITICAL FIX
    res_train = clean_array(res_train)
    res_val   = clean_array(res_val)
    res_test  = clean_array(res_test)

    Xtr, Ytr = make_seq(res_train)
    Xv, Yv = make_seq(res_val)
    Xt, Yt = make_seq(res_test)

    model = build_model((Xtr.shape[1], Xtr.shape[2]))

    model.fit(
        Xtr, Ytr,
        validation_data=(Xv, Yv),
        epochs=CONFIG["epochs"],
        batch_size=CONFIG["batch_size"],
        callbacks=[callbacks.EarlyStopping(patience=10, restore_best_weights=True)],
        verbose=1
    )

    # ---------------- Predict
    res_pred = model.predict(Xt).flatten()
    L = min(len(res_pred), len(var_test) - CONFIG["window"])

    final = var_test[CONFIG["window"]:CONFIG["window"] + L, 0] + res_pred[:L]

    dummy_pred = np.zeros((L, train_df.shape[1]))
    dummy_true = np.zeros((L, train_df.shape[1]))

    dummy_pred[:, 0] = final
    dummy_true[:, 0] = test_df.values[CONFIG["window"]:CONFIG["window"] + L, 0]

    y_pred = scaler.inverse_transform(dummy_pred)[:, 0]
    y_true = scaler.inverse_transform(dummy_true)[:, 0]

    mask = np.isfinite(y_true) & np.isfinite(y_pred)
    y_true = y_true[mask]
    y_pred = y_pred[mask]

    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    mape = mean_absolute_percentage_error(y_true, y_pred) * 100
    nrmse = rmse / (np.mean(y_true) + 1e-8)

    # ---------------- Verdicts (paper-correct)
    if name == "MMTC":
        verdict = "GOOD" if nrmse < 0.5 else "OK" if nrmse < 0.8 else "BAD"
        print(f"{name:10s} | RMSE: {rmse:.1f} | NRMSE: {nrmse:.3f} | {verdict}")
    else:
        verdict = "GOOD" if mape < 25 else "OK" if mape < 40 else "BAD"
        print(f"{name:10s} | RMSE: {rmse:.2f} | MAPE: {mape:.1f}% | {verdict}")

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    slices = load_kpi_data()
    for k, v in slices.items():
        train_slice(k, v)
