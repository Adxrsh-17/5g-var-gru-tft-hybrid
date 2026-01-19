from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("36KPI_HDFS_Streamer") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===================== KAFKA =====================
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "network-traffic") \
    .option("startingOffsets", "latest") \
    .load()

# ===================== SCHEMA =====================
schema = StructType([
    StructField("Slice_Type", StringType()),

    StructField("Total_Packets", LongType()),
    StructField("Throughput_bps", DoubleType()),
    StructField("Byte_Velocity", DoubleType()),
    StructField("Packet_Efficiency", DoubleType()),

    StructField("Avg_IAT", DoubleType()),
    StructField("Jitter", DoubleType()),
    StructField("IAT_Skewness", DoubleType()),
    StructField("IAT_Kurtosis", DoubleType()),
    StructField("IAT_PAPR", DoubleType()),
    StructField("Min_IAT", DoubleType()),
    StructField("Max_IAT", DoubleType()),
    StructField("Idle_Rate", DoubleType()),
    StructField("Transmission_Duration", DoubleType()),

    StructField("Avg_Packet_Size", DoubleType()),
    StructField("Pkt_Size_StdDev", DoubleType()),
    StructField("Pkt_Size_Skewness", DoubleType()),
    StructField("Pkt_Size_Kurtosis", DoubleType()),
    StructField("Unique_Pkt_Sizes", IntegerType()),

    StructField("Entropy_Score", DoubleType()),
    StructField("Small_Pkt_Ratio", DoubleType()),
    StructField("Large_Pkt_Ratio", DoubleType()),
    StructField("Coeff_Variation_Size", DoubleType()),

    StructField("Retransmission_Count", IntegerType()),
    StructField("Retransmission_Ratio", DoubleType()),

    StructField("Avg_Win_Size", DoubleType()),
    StructField("Win_Size_StdDev", DoubleType()),
    StructField("Min_Win_Size", DoubleType()),
    StructField("Max_Win_Size", DoubleType()),
    StructField("Win_Utilization", DoubleType()),
    StructField("Zero_Win_Count", IntegerType()),

    StructField("UDP_Ratio", DoubleType()),
    StructField("Protocol_Diversity", IntegerType()),
    StructField("IP_Source_Entropy", DoubleType()),
    StructField("Primary_IP_Ratio", DoubleType()),
    StructField("Seq_Number_Rate", DoubleType())
])

df = raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("kpi")) \
    .select("kpi.*")

# ===================== WRITE TO HDFS =====================
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/data/processed/36kpi") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/36kpi") \
    .start()

query.awaitTermination()
