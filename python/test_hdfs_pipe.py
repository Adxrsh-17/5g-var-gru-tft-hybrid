import subprocess
import pandas as pd
import io
import sys

def run_command(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise Exception(f"Command failed: {result.stderr.decode()}")
    return result.stdout.decode().strip()

try:
    print("Testing direct HDFS read via pipe...")
    
    # 1. List files
    hdfs_path = "/5g_kpi/processed/sliceType=eMBB"
    print(f"Listing {hdfs_path}...")
    
    cmd = f"hdfs dfs -ls {hdfs_path} | grep .parquet | head -n 1 | awk '{{print $8}}'"
    file_path = run_command(cmd)
    
    if not file_path:
        print("No parquet files found via ls.")
        sys.exit(1)
        
    print(f"Found file: {file_path}")
    
    # 2. Cat file content
    print("Reading content via hdfs dfs -cat...")
    cat_cmd = ["hdfs", "dfs", "-cat", file_path]
    result = subprocess.run(cat_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if result.returncode != 0:
        print(f"Cat failed: {result.stderr.decode()}")
        sys.exit(1)
        
    # 3. Load into pandas
    print(f"Read {len(result.stdout)} bytes. Parsing with pandas...")
    buffer = io.BytesIO(result.stdout)
    
    try:
        df = pd.read_parquet(buffer, engine='fastparquet')
        print(f"✅ SUCCESS! DataFrame shape: {df.shape}")
        print(df.head())
    except Exception as e:
        print(f"❌ Pandas parse failed: {e}")

except Exception as e:
    print(f"CRITICAL ERROR: {e}")
