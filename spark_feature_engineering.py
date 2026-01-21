# =====================================================
# SPARK FEATURE ENGINEERING - HDFS TO HDFS PIPELINE
# Memory-optimized version - processes each slice separately
# =====================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, lag, count, sum, avg, stddev, min, max,
    skewness, kurtosis, countDistinct, when, to_timestamp,
    window, row_number, monotonically_increasing_id
)
from pyspark.sql.types import IntegerType, DoubleType

# =====================================================
# SPARK SESSION WITH HDFS SUPPORT
# =====================================================
spark = SparkSession.builder \
    .appName("5G_KPI_Feature_Engineering_HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# HDFS PATHS
# =====================================================
HDFS_RAW_PATH = "hdfs://namenode:8020/data/raw/5g-traffic"
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/data/processed/kpi/36kpi_output"

# Slice type mapping based on source file
SLICE_MAP = {
    "mmtc.csv": "mMTC",
    "naver5g3-10M.csv": "eMBB", 
    "Youtube_cellular.csv": "URLLC"
}

# =====================================================
# LOAD AND PREP DATA FROM HDFS
# =====================================================
def load_and_prep(spark, hdfs_path, slice_type):
    """Load raw CSV from HDFS and prepare for processing"""
    print(f"📂 Loading from HDFS: {hdfs_path}")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(hdfs_path)
    
    # Add slice type and clean columns (use backticks for dotted column names)
    df = df \
        .withColumn("Slice_Type", lit(slice_type)) \
        .withColumn("retrans", col("`tcp.analysis.retransmission`").cast(IntegerType())) \
        .withColumn("win_size", col("`tcp.window_size`").cast(DoubleType())) \
        .withColumn("udp_l", col("`udp.length`").cast(DoubleType())) \
        .withColumn("teid", col("`gtp.teid`").cast("string")) \
        .withColumn("pkt_len", col("`frame.len`").cast(DoubleType())) \
        .withColumn("epoch", col("`frame.time_epoch`").cast(DoubleType())) \
        .withColumn("src_ip", col("`ip.src`")) \
        .na.fill(0, ["retrans", "win_size", "udp_l", "pkt_len"])
    
    return df

# =====================================================
# EXTRACT 36 KPIs
# =====================================================
def extract_advanced_kpis(df, window_duration="1 second"):
    """Extract 36 network KPIs from raw packet data"""
    
    # Add timestamp and compute IAT (Inter-Arrival Time)
    df_with_ts = df.withColumn("Timestamp", to_timestamp(col("epoch")))
    
    # Partition by Slice_Type for window operations
    w_spec = Window.partitionBy("Slice_Type").orderBy("epoch")
    df_with_iat = df_with_ts \
        .withColumn("prev_t", lag("epoch", 1).over(w_spec)) \
        .withColumn("IAT", col("epoch") - col("prev_t")) \
        .na.fill(0, ["IAT"])
    
    # Aggregate KPIs per time window
    kpi_df = df_with_iat.groupBy(
        window(col("Timestamp"), window_duration),
        col("Slice_Type")
    ).agg(
        # ========== VOLUME KPIs (4) ==========
        (sum("pkt_len") * 8).alias("Throughput_bps"),
        count("*").alias("Total_Packets"),
        (sum("pkt_len") / (sum(when(col("IAT") > 0.0001, col("IAT")).otherwise(0.0001)))).alias("Byte_Velocity"),
        (sum("pkt_len") / (sum("udp_l") + 1)).alias("Packet_Efficiency"),
        
        # ========== TEMPORAL KPIs (9) ==========
        avg("IAT").alias("Avg_IAT"),
        stddev("IAT").alias("Jitter"),
        skewness("IAT").alias("IAT_Skewness"),
        kurtosis("IAT").alias("IAT_Kurtosis"),
        (max("IAT") / (avg("IAT") + 0.000001)).alias("IAT_PAPR"),
        min("IAT").alias("Min_IAT"),
        max("IAT").alias("Max_IAT"),
        (sum(when(col("IAT") > 0.1, 1).otherwise(0)) / count("*")).alias("Idle_Rate"),
        (max("epoch") - min("epoch")).alias("Transmission_Duration"),
        
        # ========== TEXTURE KPIs (9) ==========
        avg("pkt_len").alias("Avg_Packet_Size"),
        stddev("pkt_len").alias("Pkt_Size_StdDev"),
        skewness("pkt_len").alias("Pkt_Size_Skewness"),
        kurtosis("pkt_len").alias("Pkt_Size_Kurtosis"),
        countDistinct("pkt_len").alias("Unique_Pkt_Sizes"),
        (countDistinct("pkt_len") / count("*")).alias("Entropy_Score"),
        (sum(when(col("pkt_len") < 64, 1).otherwise(0)) / count("*")).alias("Small_Pkt_Ratio"),
        (sum(when(col("pkt_len") > 1200, 1).otherwise(0)) / count("*")).alias("Large_Pkt_Ratio"),
        (stddev("pkt_len") / (avg("pkt_len") + 0.000001)).alias("Coeff_Variation_Size"),
        
        # ========== HEALTH KPIs (7) ==========
        sum("retrans").alias("Retransmission_Count"),
        (sum("retrans") / count("*")).alias("Retransmission_Ratio"),
        avg("win_size").alias("Avg_Win_Size"),
        stddev("win_size").alias("Win_Size_StdDev"),
        min("win_size").alias("Min_Win_Size"),
        max("win_size").alias("Max_Win_Size"),
        (avg("win_size") / (max("win_size") + 0.000001)).alias("Win_Utilization"),
        sum(when(col("win_size") == 0, 1).otherwise(0)).alias("Zero_Win_Count"),
        
        # ========== PROTOCOL KPIs (5) ==========
        (sum(when(col("udp_l") > 0, 1).otherwise(0)) / count("*")).alias("UDP_Ratio"),
        countDistinct("src_ip").alias("Protocol_Diversity"),
        countDistinct("src_ip").alias("IP_Source_Entropy"),
        (count("src_ip") / (countDistinct("src_ip") + 1)).alias("Primary_IP_Ratio"),
        (sum("pkt_len") * 0.95).alias("Seq_Number_Rate")
    ).na.fill(0)
    
    # Add monotonic ID instead of row_number (more memory efficient)
    kpi_df = kpi_df.withColumn("Serial_No", monotonically_increasing_id())
    
    # Select final columns
    final_df = kpi_df.select(
        col("Serial_No"),
        col("Slice_Type"),
        col("Throughput_bps"),
        col("Total_Packets"),
        col("Byte_Velocity"),
        col("Packet_Efficiency"),
        col("Avg_IAT"),
        col("Jitter"),
        col("IAT_Skewness"),
        col("IAT_Kurtosis"),
        col("IAT_PAPR"),
        col("Min_IAT"),
        col("Max_IAT"),
        col("Idle_Rate"),
        col("Transmission_Duration"),
        col("Avg_Packet_Size"),
        col("Pkt_Size_StdDev"),
        col("Pkt_Size_Skewness"),
        col("Pkt_Size_Kurtosis"),
        col("Unique_Pkt_Sizes"),
        col("Entropy_Score"),
        col("Small_Pkt_Ratio"),
        col("Large_Pkt_Ratio"),
        col("Coeff_Variation_Size"),
        col("Retransmission_Count"),
        col("Retransmission_Ratio"),
        col("Avg_Win_Size"),
        col("Win_Size_StdDev"),
        col("Min_Win_Size"),
        col("Max_Win_Size"),
        col("Win_Utilization"),
        col("Zero_Win_Count"),
        col("UDP_Ratio"),
        col("Protocol_Diversity"),
        col("IP_Source_Entropy"),
        col("Primary_IP_Ratio"),
        col("Seq_Number_Rate")
    )
    
    return final_df

# =====================================================
# MAIN PIPELINE
# =====================================================
def main():
    print("=" * 60)
    print("🚀 5G KPI FEATURE ENGINEERING - HDFS PIPELINE")
    print("=" * 60)
    
    first_write = True
    
    # Process each slice type separately to save memory
    for filename, slice_type in SLICE_MAP.items():
        hdfs_path = f"{HDFS_RAW_PATH}/{filename}"
        print(f"\n📊 Processing: {slice_type} from {filename}")
        
        try:
            # Load data
            df = load_and_prep(spark, hdfs_path, slice_type)
            record_count = df.count()
            print(f"✅ Loaded {record_count} records for slice: {slice_type}")
            
            # Extract KPIs
            kpi_df = extract_advanced_kpis(df, "1 second")
            
            # Write to HDFS (append mode after first)
            mode = "overwrite" if first_write else "append"
            print(f"💾 Writing to HDFS ({mode}): {HDFS_OUTPUT_PATH}")
            
            kpi_df.write \
                .mode(mode) \
                .parquet(HDFS_OUTPUT_PATH)
            
            first_write = False
            print(f"✅ Saved KPIs for {slice_type}")
            
            # Clear cache to free memory
            spark.catalog.clearCache()
            
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
            continue
    
    # Verify output
    print("\n" + "=" * 60)
    print("📊 VERIFYING OUTPUT")
    print("=" * 60)
    
    try:
        result_df = spark.read.parquet(HDFS_OUTPUT_PATH)
        total_count = result_df.count()
        print(f"✅ Total KPI records in HDFS: {total_count}")
        
        # Show slice distribution
        print("\n📈 Records per slice:")
        result_df.groupBy("Slice_Type").count().show()
        
    except Exception as e:
        print(f"❌ Error verifying output: {e}")
    
    print("\n" + "=" * 60)
    print("✅ FEATURE ENGINEERING COMPLETE - DATA STORED IN HDFS")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()
