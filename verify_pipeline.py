# =====================================================
# VERIFICATION SCRIPT - Check Pipeline Data in HDFS
# =====================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline_Verification") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("📊 HDFS PIPELINE VERIFICATION")
print("=" * 60)

# Check raw data
print("\n1️⃣ RAW DATA IN HDFS:")
try:
    raw_files = spark.read.csv("hdfs://namenode:8020/data/raw/5g-traffic/", header=True)
    print(f"   ✅ Raw records: {raw_files.count()}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Check KPI output
print("\n2️⃣ PROCESSED KPIs (Feature Engineering Output):")
try:
    kpi_df = spark.read.parquet("hdfs://namenode:8020/data/processed/kpi/36kpi_output")
    print(f"   ✅ KPI records: {kpi_df.count()}")
    print("   📈 By Slice Type:")
    kpi_df.groupBy("Slice_Type").count().show()
except Exception as e:
    print(f"   ❌ Error: {e}")

# Check streaming output
print("\n3️⃣ STREAMING OUTPUT (Consumer from Kafka):")
try:
    stream_df = spark.read.parquet("hdfs://namenode:8020/data/processed/streaming_kpi")
    print(f"   ✅ Streamed records: {stream_df.count()}")
    print("   📈 By Slice Type:")
    stream_df.groupBy("Slice_Type").count().show()
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "=" * 60)
print("✅ VERIFICATION COMPLETE")
print("=" * 60)

spark.stop()
