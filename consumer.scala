import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object NetworkConsumer {
  def main(args: Array[String]): Unit = {
    
    // 1. SETUP: Standard Spark (No HDFS Config needed for Local)
    val spark = SparkSession.builder()
      .appName("5G_KPI_EventTime_Processor")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "4") 
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    println("⏳ Starting Spark 10s Window Aggregation (Local Save)...")

    // 2. SCHEMA
    val rawSchema = new StructType()
      .add("timestamp", DoubleType)
      .add("packet_size", IntegerType)
      .add("src_ip", StringType)
      .add("dst_ip", StringType)
      .add("protocol", StringType)
      .add("tcp_window", IntegerType)
      .add("udp_len", IntegerType)
      .add("is_udp", IntegerType)
      .add("is_tcp", IntegerType)
      .add("slice_type", StringType)

    // 3. INGEST from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "network-traffic")
      .option("startingOffsets", "earliest") 
      .load()

    val parsed = kafkaStream
      .select(from_json(col("value").cast("string"), rawSchema).as("data"))
      .select("data.*")
      .withColumn("timestamp", col("timestamp").cast("timestamp"))
      .withWatermark("timestamp", "1 minute")

    // 4. PROCESS: 35 KPIs Aggregation
    val kpis = parsed.groupBy(window($"timestamp", "10 seconds"), $"slice_type")
      .agg(
        count("*").as("Total_Packets"),
        (sum("packet_size") * 8).as("Throughput_bps"),
        avg("packet_size").as("Avg_Packet_Size"),
        stddev("packet_size").as("Pkt_Size_StdDev"),
        skewness("packet_size").as("Pkt_Size_Skewness"),
        kurtosis("packet_size").as("Pkt_Size_Kurtosis"),
        (stddev("packet_size") / avg("packet_size")).as("Coeff_Variation"),
        sum("is_udp").as("UDP_Count"),
        sum("is_tcp").as("TCP_Count"),
        max("tcp_window").as("Max_Win_Size"),
        approx_count_distinct("src_ip").as("IP_Entropy")
      )

    // 5. WRITE to LOCAL FOLDER (Inside Docker)
    // This bypasses the HDFS connection error
    val query = kpis.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "/home/jovyan/5G_KPI_Output")          // <--- Local Path
      .option("checkpointLocation", "/home/jovyan/checkpoints_local") // <--- Local Path
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println("🚀 Streaming Active: Writing results to /home/jovyan/5G_KPI_Output...")
    query.awaitTermination()
  }
}