import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object NetworkConsumer {
  def main(args: Array[String]): Unit = {
    
    // 1. HIGH-PERFORMANCE CONFIG (14 CORES)
    val spark = SparkSession.builder()
      .appName("5G_KPI_36_Metrics")
      .master("local[14]")                           
      .config("spark.sql.shuffle.partitions", "28")  
      .config("spark.default.parallelism", "28")     
      .config("spark.driver.memory", "4g")           
      .config("spark.streaming.kafka.maxRatePerPartition", "5000") 
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    println("🚀 Starting 36-KPI Real-Time Engine (14 Cores)...")

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

    // 3. INGEST
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "network-traffic")
      .option("startingOffsets", "earliest") 
      .load()

    // 4. TRANSFORM
    val parsed = kafkaStream
      .select(from_json(col("value").cast("string"), rawSchema).as("data"))
      .select("data.*")
      .withColumn("timestamp", col("timestamp").cast("timestamp"))
      .withWatermark("timestamp", "30 seconds") 

    // 5. AGGREGATION (36 KPIs)
    // Note: Some complex metrics (IAT, Retransmission) use approximations 
    // because exact calculation requires stateful processing not available in standard agg.
    val kpis = parsed.groupBy(window($"timestamp", "10 seconds"), $"slice_type")
      .agg(
        // --- 1. VOLUME & VELOCITY ---
        count("*").as("Total_Packets"),
        (sum("packet_size") * 8).as("Throughput_bps"),
        (sum("packet_size") / 10).as("Byte_Velocity"), // Bytes per sec (window=10s)
        (sum("packet_size") / count("*")).as("Packet_Efficiency"), // Avg Bytes per Packet
        
        // --- 2. TIME & IAT PROXIES (Approximations) ---
        // True IAT requires row-to-row diffs. Using Window Duration / Count as proxy.
        round(lit(10.0) / count("*"), 6).as("Avg_IAT"), 
        round(stddev("packet_size") / 100, 4).as("Jitter"), // Proxy using size variance
        lit(0.0).as("IAT_Skewness"), // Requires Stateful Mapping (Too heavy for demo)
        lit(0.0).as("IAT_Kurtosis"), // Requires Stateful Mapping
        lit(0.5).as("IAT_PAPR"),     // Placeholder
        lit(0.0001).as("Min_IAT"),   // Placeholder
        lit(1.0).as("Max_IAT"),      // Placeholder
        
        // --- 3. FLOW STATE ---
        round((lit(10.0) - (count("*") * 0.001)) / 10.0, 4).as("Idle_Rate"), // Approx
        lit(10.0).as("Transmission_Duration"), // Fixed Window Size
        
        // --- 4. PACKET SIZE STATS ---
        round(avg("packet_size"), 2).as("Avg_Packet_Size"),
        round(stddev("packet_size"), 2).as("Pkt_Size_StdDev"),
        round(skewness("packet_size"), 2).as("Pkt_Size_Skewness"),
        round(kurtosis("packet_size"), 2).as("Pkt_Size_Kurtosis"),
        approx_count_distinct("packet_size").as("Unique_Pkt_Sizes"),
        
        // --- 5. ENTROPY & RATIOS ---
        // Using Unique IPs as proxy for Entropy Score
        round(approx_count_distinct("src_ip") / count("*"), 4).as("Entropy_Score"),
        
        // Small < 100 bytes, Large > 1000 bytes (Using Case When)
        round(sum(when($"packet_size" < 100, 1).otherwise(0)) / count("*"), 2).as("Small_Pkt_Ratio"),
        round(sum(when($"packet_size" > 1000, 1).otherwise(0)) / count("*"), 2).as("Large_Pkt_Ratio"),
        round(stddev("packet_size") / avg("packet_size"), 4).as("Coeff_Variation_Size"),
        
        // --- 6. RELIABILITY (Retransmission Proxy) ---
        // Real retransmission requires Seq Numbers. Using duplicates as proxy.
        (count("*") - approx_count_distinct("packet_size")).as("Retransmission_Count"), 
        round((count("*") - approx_count_distinct("packet_size")) / count("*"), 4).as("Retransmission_Ratio"),

        // --- 7. TCP WINDOW STATS ---
        round(avg("tcp_window"), 2).as("Avg_Win_Size"),
        round(stddev("tcp_window"), 2).as("Win_Size_StdDev"),
        min("tcp_window").as("Min_Win_Size"),
        max("tcp_window").as("Max_Win_Size"),
        round(avg("tcp_window") / 65535, 4).as("Win_Utilization"),
        sum(when($"tcp_window" === 0, 1).otherwise(0)).as("Zero_Win_Count"),
        
        // --- 8. PROTOCOL & HEADER ---
        round(sum("is_udp") / count("*"), 4).as("UDP_Ratio"),
        lit(2).as("Protocol_Diversity"), // TCP + UDP
        
        // Assuming Avg Header = 40 bytes
        round((count("*") * 40) / sum("packet_size"), 4).as("Header_Overhead_Ratio"),
        
        approx_count_distinct("src_ip").as("IP_Source_Entropy"),
        round(approx_count_distinct("src_ip") / count("*"), 4).as("Primary_IP_Ratio"),
        lit(0.0).as("Seq_Number_Rate") // Data missing Seq Number
      )

    // 6. OUTPUT (Horizontal Scrolling Table)
    val query = kpis.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "10") // Show top 10 rows
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
