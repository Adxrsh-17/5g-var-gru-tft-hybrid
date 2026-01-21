# =====================================================
# SPARK STREAMING CONSUMER - KAFKA TO HDFS
# Reads 36 KPI records from Kafka and writes to HDFS Parquet
# =====================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, IntegerType
)

# =====================================================
# SPARK SESSION WITH KAFKA STREAMING SUPPORT
# =====================================================
spark = SparkSession.builder \
    .appName("5G_KPI_Kafka_to_HDFS_Consumer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:8020/checkpoints/kpi") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# CONFIGURATION
# =====================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "network-traffic"
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/data/processed/streaming_kpi"
HDFS_CHECKPOINT_PATH = "hdfs://namenode:8020/checkpoints/streaming_kpi"

# =====================================================
# KPI SCHEMA (36 KPIs + Slice_Type)
# =====================================================
kpi_schema = StructType([
    StructField("Slice_Type", StringType()),
    
    # Volume KPIs (4)
    StructField("Total_Packets", LongType()),
    StructField("Throughput_bps", DoubleType()),
    StructField("Byte_Velocity", DoubleType()),
    StructField("Packet_Efficiency", DoubleType()),
    
    # Temporal KPIs (9)
    StructField("Avg_IAT", DoubleType()),
    StructField("Jitter", DoubleType()),
    StructField("IAT_Skewness", DoubleType()),
    StructField("IAT_Kurtosis", DoubleType()),
    StructField("IAT_PAPR", DoubleType()),
    StructField("Min_IAT", DoubleType()),
    StructField("Max_IAT", DoubleType()),
    StructField("Idle_Rate", DoubleType()),
    StructField("Transmission_Duration", DoubleType()),
    
    # Texture KPIs (9)
    StructField("Avg_Packet_Size", DoubleType()),
    StructField("Pkt_Size_StdDev", DoubleType()),
    StructField("Pkt_Size_Skewness", DoubleType()),
    StructField("Pkt_Size_Kurtosis", DoubleType()),
    StructField("Unique_Pkt_Sizes", IntegerType()),
    StructField("Entropy_Score", DoubleType()),
    StructField("Small_Pkt_Ratio", DoubleType()),
    StructField("Large_Pkt_Ratio", DoubleType()),
    StructField("Coeff_Variation_Size", DoubleType()),
    
    # Health KPIs (7)
    StructField("Retransmission_Count", IntegerType()),
    StructField("Retransmission_Ratio", DoubleType()),
    StructField("Avg_Win_Size", DoubleType()),
    StructField("Win_Size_StdDev", DoubleType()),
    StructField("Min_Win_Size", DoubleType()),
    StructField("Max_Win_Size", DoubleType()),
    StructField("Win_Utilization", DoubleType()),
    StructField("Zero_Win_Count", IntegerType()),
    
    # Protocol KPIs (5)
    StructField("UDP_Ratio", DoubleType()),
    StructField("Protocol_Diversity", IntegerType()),
    StructField("IP_Source_Entropy", DoubleType()),
    StructField("Primary_IP_Ratio", DoubleType()),
    StructField("Seq_Number_Rate", DoubleType())
])

# =====================================================
# MAIN STREAMING PIPELINE
# =====================================================
def main():
    print("=" * 60)
    print("🚀 KAFKA CONSUMER - STREAMING TO HDFS")
    print("=" * 60)
    
    # Read from Kafka
    print(f"\n📡 Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📥 Subscribing to topic: {KAFKA_TOPIC}")
    
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON from Kafka value
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), kpi_schema).alias("kpi")) \
        .select("kpi.*")
    
    print(f"\n💾 Writing to HDFS: {HDFS_OUTPUT_PATH}")
    print(f"📁 Checkpoint: {HDFS_CHECKPOINT_PATH}")
    
    # Write to HDFS as Parquet
    query = parsed_stream.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", HDFS_OUTPUT_PATH) \
        .option("checkpointLocation", HDFS_CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("\n✅ Streaming started! Waiting for data...")
    print("   Press Ctrl+C to stop")
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
