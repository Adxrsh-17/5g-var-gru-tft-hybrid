// KafkaKpiPipeline.scala
// PHASE 3: PRODUCTION - BATCH MODE (Fixes Watermark Deadlock)
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Encoder, Encoders, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp

object StreamingKpiConfig extends Serializable {
  val KAFKA_BOOTSTRAP: String = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
  val KAFKA_TOPIC: String = sys.env.getOrElse("KAFKA_PACKET_TOPIC", "5g-packet-events")
  val HDFS_BASE: String = "/5G_kpi"
  val HDFS_OUTPUT: String = s"$HDFS_BASE/final/kpi_parquet"
  
  val SHUFFLE_PARTITIONS: String = sys.env.getOrElse("SHUFFLE_PARTITIONS", "100")
  val WINDOW_DURATION: String = "5 seconds"
  val IDLE_THRESHOLD: Double = 0.1
  val SMALL_PKT: Int = 100
  val LARGE_PKT: Int = 1400
  val EPS: Double = 1e-6
}

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

case class PacketInput(
  sliceType: String, fileName: String, timestamp: Double, timestampMs: Long,
  packetLen: Int, capturedLen: Int, protocol: String,
  srcIp: String, dstIp: String, srcPort: Int, dstPort: Int, flowId: String,
  ipHeaderLen: Int, tcpFlags: Int, windowSize: Int, seqNumber: Long,
  eventTime: Timestamp
)

case class PacketWithIAT(
  sliceType: String, fileName: String, timestamp: Double, flowId: String,
  packetLen: Int, protocol: String, srcPort: Int, dstPort: Int,
  tcpFlags: Int, windowSize: Int, eventTime: Timestamp,
  IAT: Double
)

case class FlowKey(sliceType: String, flowId: String)

object BatchIATComputer {
  // BATCH VERSION: Standard Iterator processing, no GroupState needed
  def computeIAT(key: FlowKey, packets: Iterator[PacketInput]): Iterator[PacketWithIAT] = {
    // 1. Collect and Sort packets by timestamp to ensure correct IAT calculation
    val sortedPackets = packets.toSeq.sortBy(_.timestamp)
    var lastTs = 0.0
    val results = new scala.collection.mutable.ArrayBuffer[PacketWithIAT](sortedPackets.size)
    
    sortedPackets.foreach { p =>
      // IAT is diff between current and last packet in this flow
      val iat = if (lastTs > 0.0) Math.max(0.0, p.timestamp - lastTs) else 0.0
      lastTs = p.timestamp
      
      results += PacketWithIAT(
        p.sliceType, p.fileName, p.timestamp, p.flowId,
        p.packetLen, p.protocol, p.srcPort, p.dstPort,
        p.tcpFlags, p.windowSize, p.eventTime,
        iat
      )
    }
    results.iterator
  }
}

object KpiComputer extends Serializable {
  def compute36KPIs(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val idleThreshold = StreamingKpiConfig.IDLE_THRESHOLD
    val smallPkt = StreamingKpiConfig.SMALL_PKT
    val largePkt = StreamingKpiConfig.LARGE_PKT
    val eps = StreamingKpiConfig.EPS
    
    // Aggregation Logic (Same as before, but runs on Batch DataFrame)
    df.groupBy(col("sliceType"), window(col("eventTime"), StreamingKpiConfig.WINDOW_DURATION).as("window"))
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
        approx_count_distinct("packetLen", 0.05).alias("Unique_Pkt_Sizes"),
        (sum(when(col("packetLen") < lit(smallPkt), 1).otherwise(0)) / count("*")).alias("Small_Pkt_Ratio"),
        (sum(when(col("packetLen") > lit(largePkt), 1).otherwise(0)) / count("*")).alias("Large_Pkt_Ratio"),
        (sum(when(col("protocol") === "TCP", 1).otherwise(0)) / count("*")).alias("TCP_Ratio"),
        (sum(when(col("protocol") === "UDP", 1).otherwise(0)) / count("*")).alias("UDP_Ratio"),
        approx_count_distinct("protocol", 0.05).alias("Protocol_Diversity"),
        approx_count_distinct("srcPort", 0.05).alias("Unique_Src_Ports"),
        avg("windowSize").alias("Avg_Win_Size"),
        stddev("windowSize").alias("Win_Size_StdDev"),
        min("windowSize").alias("Min_Win_Size"),
        max("windowSize").alias("Max_Win_Size"),
        sum(when(col("windowSize") === 0, 1).otherwise(0)).alias("Zero_Win_Count"),
        sum(when(col("tcpFlags").bitwiseAND(lit(0x04)) =!= 0, 1).otherwise(0)).alias("RST_Count"),
        approx_count_distinct("dstPort", 0.05).alias("Unique_Dst_Ports"),
        (stddev("packetLen") / (avg("packetLen") + lit(eps))).alias("Coeff_Variation_Size")
      )
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .drop("window")
      .na.fill(0.0)
  }
}

object KafkaKpiPipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_KPI_Batch_Processor")
      .config("spark.sql.shuffle.partitions", StreamingKpiConfig.SHUFFLE_PARTITIONS)
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "false")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    println(s"\n🚀 STARTING BATCH PIPELINE (Reading all Kafka Data)")
    
    // 1. READ (Batch Mode)
    // "startingOffsets"="earliest", "endingOffsets"="latest" reads ALL current data
    val rawDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", StreamingKpiConfig.KAFKA_BOOTSTRAP)
      .option("subscribe", StreamingKpiConfig.KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest") 
      .load()

    val typedStream = rawDF
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", PacketEventSchema.schema).as("data"))
      .select($"data.*")
      .filter($"sliceType".isNotNull)
      // Use current processing time to ensure recent bucket aggregation
      .withColumn("eventTime", current_timestamp())
      .as[PacketInput]

    // 2. PROCESS (Stateful IAT Calculation)
    // In batch, we use flatMapGroups (standard Dataset API) instead of flatMapGroupsWithState
    val processedDS = typedStream
      .groupByKey(p => FlowKey(p.sliceType, p.flowId))
      .flatMapGroups(BatchIATComputer.computeIAT)
      .toDF()

    // 3. AGGREGATE (KPIs)
    val kpiDF = KpiComputer.compute36KPIs(processedDS, spark)

    println(s"💾 WRITING KPIs to HDFS: ${StreamingKpiConfig.HDFS_OUTPUT}")

    // 4. WRITE (Standard Parquet Write)
    // No checkpoints needed. No watermarks waiting. Just write.
    kpiDF.write
      .mode(SaveMode.Append)
      .partitionBy("sliceType")
      .parquet(StreamingKpiConfig.HDFS_OUTPUT)

    println(s"✅ BATCH COMPLETION: Data successfully written to HDFS.")
    spark.stop()
  }
}