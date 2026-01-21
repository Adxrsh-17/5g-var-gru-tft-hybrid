# =====================================================
# KAFKA PRODUCER - READS FROM HDFS (NO LOCAL STORAGE)
# Streams 36 KPI records from HDFS to Kafka topic
# =====================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col

# =====================================================
# SPARK SESSION WITH KAFKA SUPPORT
# =====================================================
spark = SparkSession.builder \
    .appName("5G_KPI_HDFS_to_Kafka_Producer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# CONFIGURATION
# =====================================================
HDFS_KPI_PATH = "hdfs://namenode:8020/data/processed/kpi/36kpi_output"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "network-traffic"

# 36 KPI Columns
KPI_COLUMNS = [
    "Slice_Type", "Total_Packets", "Throughput_bps", "Byte_Velocity",
    "Packet_Efficiency", "Avg_IAT", "Jitter", "IAT_Skewness", "IAT_Kurtosis",
    "IAT_PAPR", "Min_IAT", "Max_IAT", "Idle_Rate", "Transmission_Duration",
    "Avg_Packet_Size", "Pkt_Size_StdDev", "Pkt_Size_Skewness", "Pkt_Size_Kurtosis",
    "Unique_Pkt_Sizes", "Entropy_Score", "Small_Pkt_Ratio", "Large_Pkt_Ratio",
    "Coeff_Variation_Size", "Retransmission_Count", "Retransmission_Ratio",
    "Avg_Win_Size", "Win_Size_StdDev", "Min_Win_Size", "Max_Win_Size",
    "Win_Utilization", "Zero_Win_Count", "UDP_Ratio", "Protocol_Diversity",
    "IP_Source_Entropy", "Primary_IP_Ratio", "Seq_Number_Rate"
]

# =====================================================
# MAIN PIPELINE
# =====================================================
def main():
    print("=" * 60)
    print("🚀 KAFKA PRODUCER - STREAMING FROM HDFS")
    print("=" * 60)
    
    # Read KPI data from HDFS (Parquet format)
    print(f"\n📂 Reading KPI data from HDFS: {HDFS_KPI_PATH}")
    
    try:
        df = spark.read.parquet(HDFS_KPI_PATH)
        print(f"✅ Loaded {df.count()} KPI records from HDFS")
    except Exception as e:
        print(f"❌ Error reading from HDFS: {e}")
        print("📝 Trying CSV format...")
        csv_path = HDFS_KPI_PATH.replace("36kpi_output", "36kpi_csv")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        print(f"✅ Loaded {df.count()} KPI records from HDFS CSV")
    
    # Select only KPI columns that exist
    available_cols = [c for c in KPI_COLUMNS if c in df.columns]
    df = df.select(available_cols)
    
    print(f"📊 Streaming {len(available_cols)} KPI columns to Kafka topic: {KAFKA_TOPIC}")
    
    # Convert to JSON and send to Kafka
    # Create a struct with all KPI columns, then convert to JSON
    json_df = df.select(
        to_json(struct([col(c) for c in available_cols])).alias("value")
    )
    
    # Write to Kafka
    print(f"\n📡 Sending to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    
    json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .save()
    
    print("\n" + "=" * 60)
    print("✅ STREAMING COMPLETE - ALL KPI DATA SENT TO KAFKA")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()
