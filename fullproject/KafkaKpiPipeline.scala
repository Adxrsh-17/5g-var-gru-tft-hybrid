// KafkaKpiPipeline.scala
// Phase 3: Structured Streaming KPI Pipeline with Event-Time Semantics
// Architecture: Kafka (Packet Events) → Spark Streaming → 36 KPIs → HDFS (Checkpointed)

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.{Trigger, OutputMode}
import org.apache.spark.sql.types._

// ============================================
// CONFIGURATION MODULE
// ============================================
object StreamingKpiConfig extends Serializable {
  // Kafka Configuration
  val KAFKA_BOOTSTRAP_SERVERS: String = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
  val KAFKA_PACKET_TOPIC: String = sys.env.getOrElse("KAFKA_PACKET_TOPIC", "5g-packet-events")
  
  // HDFS Paths
  val HDFS_BASE: String = "hdfs://namenode:8020/5G_kpi"
  val HDFS_CHECKPOINT: String = s"$HDFS_BASE/checkpoints/streaming_kpi"
  val HDFS_OUTPUT: String = s"$HDFS_BASE/final/kpi_streaming"
  
  // KPI Computation Parameters
  val IDLE_THRESHOLD: Double = sys.env.getOrElse("KPI_IDLE_THRESHOLD", "0.1").toDouble
  val SMALL_PKT_THRESHOLD: Int = sys.env.getOrElse("KPI_SMALL_PKT", "100").toInt
  val LARGE_PKT_THRESHOLD: Int = sys.env.getOrElse("KPI_LARGE_PKT", "1400").toInt
  val EPS: Double = 1e-6
  
  // Streaming Parameters
  val WINDOW_DURATION: String = "1 second"
  val WATERMARK_DELAY: String = "10 seconds"
  val TRIGGER_INTERVAL: String = "5 seconds"
}

// ============================================
// PACKET EVENT SCHEMA (from Kafka)
// ============================================
object PacketEventSchema {
  val schema = StructType(Seq(
    StructField("sliceType", StringType, nullable = false),
    StructField("fileName", StringType, nullable = true),
    StructField("timestamp", DoubleType, nullable = false),
    StructField("timestampMs", LongType, nullable = false),
    StructField("packetLen", IntegerType, nullable = false),
    StructField("capturedLen", IntegerType, nullable = false),
    StructField("protocol", StringType, nullable = false),
    StructField("srcIp", StringType, nullable = false),
    StructField("dstIp", StringType, nullable = false),
    StructField("srcPort", IntegerType, nullable = false),
    StructField("dstPort", IntegerType, nullable = false),
    StructField("flowId", StringType, nullable = false),
    StructField("ipHeaderLen", IntegerType, nullable = true),
    StructField("tcpFlags", IntegerType, nullable = true),
    StructField("windowSize", IntegerType, nullable = true),
    StructField("seqNumber", LongType, nullable = true)
  ))
}

// ============================================
// KPI COMPUTATION MODULE
// ============================================
object KpiComputer extends Serializable {
  
  /**
   * Computes 36 KPIs with proper flow-based temporal metrics
   * Uses event-time semantics with watermarking
   */
  def compute36KPIs(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    val idleThreshold = StreamingKpiConfig.IDLE_THRESHOLD
    val smallPkt = StreamingKpiConfig.SMALL_PKT_THRESHOLD
    val largePkt = StreamingKpiConfig.LARGE_PKT_THRESHOLD
    val eps = StreamingKpiConfig.EPS
    
    // Add event timestamp for watermarking
    val withEventTime = df
      .withColumn("eventTime", (col("timestamp").cast("double") * 1000).cast("timestamp"))
    
    // Apply watermark for late data handling
    val watermarked = withEventTime
      .withWatermark("eventTime", StreamingKpiConfig.WATERMARK_DELAY)
    
    // Compute IAT (Inter-Arrival Time) per flow
    val flowWindowSpec = Window
      .partitionBy("sliceType", "flowId")
      .orderBy("eventTime")
    
    val withIAT = watermarked
      .withColumn("prevTimestamp", lag("timestamp", 1).over(flowWindowSpec))
      .withColumn("IAT", 
        when(col("prevTimestamp").isNotNull, 
          col("timestamp") - col("prevTimestamp")
        ).otherwise(0.0)
      )
    
    // Window aggregation: 1-second tumbling windows
    val kpiDF = withIAT
      .groupBy(
        col("sliceType"),
        window(col("eventTime"), StreamingKpiConfig.WINDOW_DURATION).as("window")
      )
      .agg(
        // VOLUME KPIs
        (sum("packetLen") * 8).alias("Throughput_bps"),
        count("*").alias("Total_Packets"),
        sum("packetLen").alias("Total_Bytes"),
        (sum("packetLen") / (sum("IAT") + lit(eps))).alias("Byte_Velocity"),
        
        // TEMPORAL KPIs
        avg("IAT").alias("Avg_IAT"),
        stddev("IAT").alias("Jitter"),
        skewness("IAT").alias("IAT_Skewness"),
        kurtosis("IAT").alias("IAT_Kurtosis"),
        min("IAT").alias("Min_IAT"),
        max("IAT").alias("Max_IAT"),
        (max("IAT") / (avg("IAT") + lit(eps))).alias("IAT_PAPR"),
        (max("timestamp") - min("timestamp")).alias("Transmission_Duration"),
        sum(when(col("IAT") > lit(idleThreshold), 1).otherwise(0)).alias("Idle_Periods"),
        (sum(when(col("IAT") > lit(idleThreshold), 1).otherwise(0)) / count("*")).alias("Idle_Rate"),
        percentile_approx(col("IAT"), lit(0.5), lit(100)).alias("IAT_Median"),
        
        // PACKET SIZE KPIs
        avg("packetLen").alias("Avg_Packet_Size"),
        stddev("packetLen").alias("Pkt_Size_StdDev"),
        skewness("packetLen").alias("Pkt_Size_Skewness"),
        kurtosis("packetLen").alias("Pkt_Size_Kurtosis"),
        min("packetLen").alias("Min_Pkt_Size"),
        max("packetLen").alias("Max_Pkt_Size"),
        countDistinct("packetLen").alias("Unique_Pkt_Sizes"),
        (sum(when(col("packetLen") < lit(smallPkt), 1).otherwise(0)) / count("*")).alias("Small_Pkt_Ratio"),
        (sum(when(col("packetLen") > lit(largePkt), 1).otherwise(0)) / count("*")).alias("Large_Pkt_Ratio"),
        
        // PROTOCOL KPIs
        (sum(when(col("protocol") === "TCP", 1).otherwise(0)) / count("*")).alias("TCP_Ratio"),
        (sum(when(col("protocol") === "UDP", 1).otherwise(0)) / count("*")).alias("UDP_Ratio"),
        countDistinct("protocol").alias("Protocol_Diversity"),
        countDistinct("srcPort").alias("Unique_Src_Ports"),
        
        // TCP HEALTH KPIs
        avg("windowSize").alias("Avg_Win_Size"),
        stddev("windowSize").alias("Win_Size_StdDev"),
        min("windowSize").alias("Min_Win_Size"),
        max("windowSize").alias("Max_Win_Size"),
        sum(when(col("windowSize") === 0, 1).otherwise(0)).alias("Zero_Win_Count"),
        sum(when(col("tcpFlags").bitwiseAND(lit(0x04)) =!= 0, 1).otherwise(0)).alias("RST_Count"),
        
        // FLOW KPIs
        countDistinct("dstPort").alias("Unique_Dst_Ports"),
        (stddev("packetLen") / (avg("packetLen") + lit(eps))).alias("Coeff_Variation_Size")
      )
      // FIX: Use withColumn to extract window start/end instead of selecting '*'
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .drop("window")
      .na.fill(0.0)
    
    kpiDF
  }
}

// ============================================
// MAIN STREAMING APPLICATION
// ============================================
object KafkaKpiPipeline {

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("5G_KPI_Structured_Streaming")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.streaming.checkpointLocation", StreamingKpiConfig.HDFS_CHECKPOINT)
      .config("spark.sql.adaptive.enabled", "true")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    val kafkaBootstrap = StreamingKpiConfig.KAFKA_BOOTSTRAP_SERVERS
    val kafkaTopic = StreamingKpiConfig.KAFKA_PACKET_TOPIC
    val checkpointPath = StreamingKpiConfig.HDFS_CHECKPOINT
    val outputPath = StreamingKpiConfig.HDFS_OUTPUT
    
    println("\n" + "="*60)
    println("   5G KPI PIPELINE (BATCH MODE FOR 23GB)")
    println("="*60)
    
    try {
        println("\n🚀 STARTING BATCH PIPELINE (Reading all Kafka Data)")
        
        // 1. Read ALL data from Kafka (Batch Mode)
        val batchDF = spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrap)
            .option("subscribe", kafkaTopic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) as json_value")
            .withColumn("parsed", from_json(col("json_value"), PacketEventSchema.schema))
            .select(col("parsed.*"))
            .filter(col("sliceType").isNotNull)
          
        println(s"   ✅ Read ${batchDF.count()} packet events in batch mode")
          
        // 2. Compute KPIs
        val batchKpiDF = computeBatchKPIs(batchDF, spark)
        
        // 3. Write to HDFS
        println(s"\n💾 WRITING KPIs to HDFS: $outputPath")
        batchKpiDF.write
            .mode("overwrite")
            .partitionBy("sliceType")
            .parquet(outputPath)
          
        println(s"   ✅ SUCCESS! KPIs written to $outputPath")
          
    } catch {
        case e: Exception =>
            println(s"\n❌ PIPELINE FAILED: ${e.getMessage}")
            e.printStackTrace()
    }
    
    spark.stop()
  }
  
  // ========================================
  // BATCH MODE KPI COMPUTATION
  // ========================================
  def computeBatchKPIs(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    val idleThreshold = StreamingKpiConfig.IDLE_THRESHOLD
    val smallPkt = StreamingKpiConfig.SMALL_PKT_THRESHOLD
    val largePkt = StreamingKpiConfig.LARGE_PKT_THRESHOLD
    val eps = StreamingKpiConfig.EPS
    
    val flowWindowSpec = Window
      .partitionBy("sliceType", "flowId")
      .orderBy("timestamp")
    
    val withIAT = df
      .withColumn("prevTimestamp", lag("timestamp", 1).over(flowWindowSpec))
      .withColumn("IAT", 
        when(col("prevTimestamp").isNotNull, 
          col("timestamp") - col("prevTimestamp")
        ).otherwise(0.0)
      )
      .withColumn("windowStart", floor(col("timestamp")).cast(LongType))
    
    val kpiDF = withIAT
      .groupBy("sliceType", "windowStart")
      .agg(
        (sum("packetLen") * 8).alias("Throughput_bps"),
        count("*").alias("Total_Packets"),
        sum("packetLen").alias("Total_Bytes"),
        (sum("packetLen") / (sum("IAT") + lit(eps))).alias("Byte_Velocity"),
        avg("IAT").alias("Avg_IAT"),
        stddev("IAT").alias("Jitter"),
        skewness("IAT").alias("IAT_Skewness"),
        kurtosis("IAT").alias("IAT_Kurtosis"),
        min("IAT").alias("Min_IAT"),
        max("IAT").alias("Max_IAT"),
        (max("IAT") / (avg("IAT") + lit(eps))).alias("IAT_PAPR"),
        (max("timestamp") - min("timestamp")).alias("Transmission_Duration"),
        sum(when(col("IAT") > lit(idleThreshold), 1).otherwise(0)).alias("Idle_Periods"),
        (sum(when(col("IAT") > lit(idleThreshold), 1).otherwise(0)) / count("*")).alias("Idle_Rate"),
        percentile_approx(col("IAT"), lit(0.5), lit(100)).alias("IAT_Median"),
        avg("packetLen").alias("Avg_Packet_Size"),
        stddev("packetLen").alias("Pkt_Size_StdDev"),
        skewness("packetLen").alias("Pkt_Size_Skewness"),
        kurtosis("packetLen").alias("Pkt_Size_Kurtosis"),
        min("packetLen").alias("Min_Pkt_Size"),
        max("packetLen").alias("Max_Pkt_Size"),
        countDistinct("packetLen").alias("Unique_Pkt_Sizes"),
        (sum(when(col("packetLen") < lit(smallPkt), 1).otherwise(0)) / count("*")).alias("Small_Pkt_Ratio"),
        (sum(when(col("packetLen") > lit(largePkt), 1).otherwise(0)) / count("*")).alias("Large_Pkt_Ratio"),
        (sum(when(col("protocol") === "TCP", 1).otherwise(0)) / count("*")).alias("TCP_Ratio"),
        (sum(when(col("protocol") === "UDP", 1).otherwise(0)) / count("*")).alias("UDP_Ratio"),
        countDistinct("protocol").alias("Protocol_Diversity"),
        countDistinct("srcPort").alias("Unique_Src_Ports"),
        avg("windowSize").alias("Avg_Win_Size"),
        stddev("windowSize").alias("Win_Size_StdDev"),
        min("windowSize").alias("Min_Win_Size"),
        max("windowSize").alias("Max_Win_Size"),
        sum(when(col("windowSize") === 0, 1).otherwise(0)).alias("Zero_Win_Count"),
        sum(when(col("tcpFlags").bitwiseAND(lit(0x04)) =!= 0, 1).otherwise(0)).alias("RST_Count"),
        countDistinct("dstPort").alias("Unique_Dst_Ports"),
        (stddev("packetLen") / (avg("packetLen") + lit(eps))).alias("Coeff_Variation_Size")
      )
      .na.fill(0.0)
      // FIX: Duplicate column selection removed. 
      // sliceType and windowStart are already preserved by groupBy.
    
    kpiDF
  }
}