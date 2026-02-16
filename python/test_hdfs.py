import os
import tensorflow as tf
import pandas as pd

try:
    print("Testing tf.io.gfile access to HDFS...")
    path = "hdfs://namenode:8020/5g_kpi/processed/*"
    files = tf.io.gfile.glob(path)
    print(f"Found {len(files)} files via glob")
    
    if files:
        f = files[0]
        print(f"Attempting to read: {f}")
        with tf.io.gfile.GFile(f, "rb") as gf:
            try:
                df = pd.read_parquet(gf, engine='fastparquet')
                print(f"Successfully read parquet with shape: {df.shape}")
            except Exception as e:
                print(f"Pandas read failed: {e}")
    else:
        print("No files found.")

except Exception as e:
    print(f"CRITICAL ERROR: {e}")
