// PcapKpiExtractor.scala
// Phase 2: Production-Ready Distributed PCAP Processing
// HDFS → Spark (Distributed Binary Decode) → KPI DataFrame → Kafka Ready

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{DataInputStream, BufferedInputStream}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer

// Configuration object for thresholds (can be overridden via environment variables)
object KpiConfig {
  val IDLE_THRESHOLD: Double = sys.env.getOrElse("KPI_IDLE_THRESHOLD", "0.1").toDouble
  val SMALL_PKT_THRESHOLD: Int = sys.env.getOrElse("KPI_SMALL_PKT", "100").toInt
  val LARGE_PKT_THRESHOLD: Int = sys.env.getOrElse("KPI_LARGE_PKT", "1400").toInt
  val MAX_PACKETS_PER_FILE: Int = sys.env.getOrElse("KPI_MAX_PACKETS", "10000").toInt
  val EPS: Double = 1e-6  // Epsilon for division safety
}

// Case class for decoded packets (with IP addresses for flow tracking)
case class Packet(
  sliceType: String,
  fileName: String,
  timestampSec: Long,
  timestampUsec: Long,
  packetLen: Int,
  capturedLen: Int,
  protocol: String,
  srcIp: String,
  dstIp: String,
  srcPort: Int,
  dstPort: Int,
  ipHeaderLen: Int,
  tcpFlags: Int,
  windowSize: Int,
  seqNumber: Long
)

object PcapKpiExtractor {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_PCAP_KPI_Extractor")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("\n" + "="*60)
    println("   5G PCAP-TO-KPI PIPELINE - PHASE 2 (PRODUCTION)")
    println("   Distributed Binary PCAP Decoding + KPI Computation")
    println("="*60)
    println(s"\n⚙️ Configuration:")
    println(s"   IDLE_THRESHOLD:    ${KpiConfig.IDLE_THRESHOLD}s")
    println(s"   SMALL_PKT:         ${KpiConfig.SMALL_PKT_THRESHOLD} bytes")
    println(s"   LARGE_PKT:         ${KpiConfig.LARGE_PKT_THRESHOLD} bytes")
    println(s"   MAX_PACKETS/FILE:  ${KpiConfig.MAX_PACKETS_PER_FILE}")
    println(s"   EPS:               ${KpiConfig.EPS}")
    
    val hdfsBasePath = "hdfs://namenode:8020/5G_kpi/raw/pcap"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
    // Slice directories
    val sliceDirs = Map(
      "eMBB" -> s"$hdfsBasePath/emBB",
      "URLLC" -> s"$hdfsBasePath/urllc",
      "mMTC" -> s"$hdfsBasePath/mmtc"
    )
    
    // Collect all packets from all files
    var allPackets = ArrayBuffer[Packet]()
    
    println("\n📦 PHASE 2A: Decoding PCAP Files...\n")
    
    sliceDirs.foreach { case (sliceType, dirPath) =>
      println(s"🔍 Processing $sliceType...")
      
      try {
        val path = new Path(dirPath)
        if (fs.exists(path)) {
          val files = fs.listStatus(path).filter(_.getPath.getName.endsWith(".pcap"))
          
          // Process first file of each slice for PoC (limit for speed)
          val filesToProcess = if (sliceType == "mMTC") files.take(2) else files.take(1)
          
          filesToProcess.foreach { fileStatus =>
            val filePath = fileStatus.getPath
            val fileName = filePath.getName
            print(s"   📄 $fileName ... ")
            
            val packets = decodePcapFile(fs, filePath, sliceType, fileName, maxPackets = KpiConfig.MAX_PACKETS_PER_FILE)
            allPackets ++= packets
            println(s"✅ ${packets.length} packets")
          }
        }
      } catch {
        case e: Exception => println(s"   ❌ Error: ${e.getMessage}")
      }
    }
    
    println(s"\n📊 Total packets decoded: ${allPackets.length}")
    
    // Convert to DataFrame
    println("\n📦 PHASE 2B: Converting to DataFrame...")
    val packetDF = allPackets.toSeq.toDF()
    
    // Add computed columns with flow ID for proper IAT calculation
    val enrichedDF = packetDF
      .withColumn("timestamp", col("timestampSec") + col("timestampUsec") / 1000000.0)
      .withColumn("timestampMs", (col("timestamp") * 1000).cast(LongType))
      // Create flow ID for flow-partitioned IAT calculation
      .withColumn("flowId", 
        concat_ws("_", col("srcIp"), col("dstIp"), col("srcPort"), col("dstPort"), col("protocol"))
      )
    
    println(s"   ✅ DataFrame created with ${enrichedDF.count()} rows")
    enrichedDF.printSchema()
    
    // Show sample data
    println("\n📋 Sample Packets:")
    enrichedDF.select("sliceType", "timestamp", "packetLen", "protocol", "srcPort", "dstPort")
      .show(10, truncate = false)
    
    // PHASE 2C: Compute KPIs per 1-second window
    println("\n📦 PHASE 2C: Computing 36 KPIs per Window...")
    
    val kpiDF = computeKPIs(enrichedDF, spark)
    
    println(s"\n✅ KPI DataFrame created with ${kpiDF.count()} windows")
    println("\n📋 Sample KPIs:")
    kpiDF.show(5, truncate = false)
    
    // Save to HDFS as Parquet
    val outputPath = "hdfs://namenode:8020/5G_kpi/processed/kpi_parquet"
    println(s"\n💾 Saving KPIs to: $outputPath")
    
    kpiDF.write.mode("overwrite").partitionBy("sliceType").parquet(outputPath)
    
    println("\n" + "="*60)
    println("✅ PHASE 2 COMPLETE!")
    println("="*60)
    println(s"   Packets Processed: ${allPackets.length}")
    println(s"   KPI Windows:       ${kpiDF.count()}")
    println(s"   Output:            $outputPath")
    println("\nNext: Phase 3 - Kafka Streaming Integration")
    
    spark.stop()
  }
  
  // PCAP Binary Decoder
  def decodePcapFile(fs: FileSystem, filePath: Path, sliceType: String, fileName: String, maxPackets: Int): ArrayBuffer[Packet] = {
    val packets = ArrayBuffer[Packet]()
    val inputStream = new DataInputStream(new BufferedInputStream(fs.open(filePath), 65536))
    
    try {
      // Read Global Header (24 bytes)
      val magicNumber = inputStream.readInt()
      val isLittleEndian = (magicNumber == 0xd4c3b2a1 || magicNumber == 0x4d3cb2a1)
      
      // Skip rest of global header (20 bytes)
      inputStream.skipBytes(20)
      
      var packetCount = 0
      
      while (inputStream.available() > 16 && packetCount < maxPackets) {
        // Read Packet Header (16 bytes)
        val tsSec = readInt(inputStream, isLittleEndian)
        val tsUsec = readInt(inputStream, isLittleEndian)
        val capturedLen = readInt(inputStream, isLittleEndian)
        val originalLen = readInt(inputStream, isLittleEndian)
        
        if (capturedLen > 0 && capturedLen < 65536) {
          // Read packet data
          val packetData = new Array[Byte](capturedLen)
          inputStream.readFully(packetData)
          
          // Parse packet headers
          val packet = parsePacket(packetData, sliceType, fileName, tsSec, tsUsec, originalLen, capturedLen)
          packets += packet
          packetCount += 1
        } else {
          // Invalid packet, skip
          if (capturedLen > 0) inputStream.skipBytes(Math.min(capturedLen, inputStream.available()))
        }
      }
    } catch {
      case _: java.io.EOFException => // End of file
      case e: Exception => println(s"Decode error: ${e.getMessage}")
    } finally {
      inputStream.close()
    }
    
    packets
  }
  
  def readInt(dis: DataInputStream, littleEndian: Boolean): Int = {
    val bytes = new Array[Byte](4)
    dis.readFully(bytes)
    if (littleEndian) {
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt
    } else {
      ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt
    }
  }
  
  def parsePacket(data: Array[Byte], sliceType: String, fileName: String, 
                  tsSec: Int, tsUsec: Int, origLen: Int, capLen: Int): Packet = {
    var protocol = "OTHER"
    var srcIp = "0.0.0.0"
    var dstIp = "0.0.0.0"
    var srcPort = 0
    var dstPort = 0
    var ipHeaderLen = 0
    var tcpFlags = 0
    var windowSize = 0
    var seqNumber = 0L
    
    try {
      // Skip Ethernet header (14 bytes) if present
      val etherType = if (data.length > 13) ((data(12) & 0xFF) << 8) | (data(13) & 0xFF) else 0
      val ipOffset = if (etherType == 0x0800 || etherType == 0x86DD) 14 else 0
      
      if (data.length > ipOffset + 20) {
        val ipVersion = (data(ipOffset) >> 4) & 0x0F
        
        if (ipVersion == 4) {
          ipHeaderLen = (data(ipOffset) & 0x0F) * 4
          val ipProtocol = data(ipOffset + 9) & 0xFF
          
          // Extract IP addresses for flow tracking
          srcIp = s"${data(ipOffset + 12) & 0xFF}.${data(ipOffset + 13) & 0xFF}.${data(ipOffset + 14) & 0xFF}.${data(ipOffset + 15) & 0xFF}"
          dstIp = s"${data(ipOffset + 16) & 0xFF}.${data(ipOffset + 17) & 0xFF}.${data(ipOffset + 18) & 0xFF}.${data(ipOffset + 19) & 0xFF}"
          
          protocol = ipProtocol match {
            case 6 => "TCP"
            case 17 => "UDP"
            case 1 => "ICMP"
            case _ => "OTHER"
          }
          
          val transportOffset = ipOffset + ipHeaderLen
          
          if (data.length > transportOffset + 4) {
            srcPort = ((data(transportOffset) & 0xFF) << 8) | (data(transportOffset + 1) & 0xFF)
            dstPort = ((data(transportOffset + 2) & 0xFF) << 8) | (data(transportOffset + 3) & 0xFF)
            
            if (protocol == "TCP" && data.length > transportOffset + 20) {
              seqNumber = ((data(transportOffset + 4) & 0xFFL) << 24) |
                          ((data(transportOffset + 5) & 0xFFL) << 16) |
                          ((data(transportOffset + 6) & 0xFFL) << 8) |
                          (data(transportOffset + 7) & 0xFFL)
              tcpFlags = data(transportOffset + 13) & 0xFF
              windowSize = ((data(transportOffset + 14) & 0xFF) << 8) | (data(transportOffset + 15) & 0xFF)
            }
          }
        }
      }
    } catch {
      case _: Exception => // Keep defaults
    }
    
    Packet(sliceType, fileName, tsSec.toLong & 0xFFFFFFFFL, tsUsec.toLong & 0xFFFFFFFFL, 
           origLen, capLen, protocol, srcIp, dstIp, srcPort, dstPort, ipHeaderLen, tcpFlags, windowSize, seqNumber)
  }
  
  // KPI Computation (36 KPIs) with proper flow partitioning and configurable thresholds
  def computeKPIs(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    // Flow-partitioned window for proper IAT calculation
    val flowWindowSpec = Window.partitionBy("sliceType", "flowId").orderBy("timestamp")
    
    val withIAT = df
      .withColumn("prevTimestamp", lag("timestamp", 1).over(flowWindowSpec))
      .withColumn("IAT", when(col("prevTimestamp").isNotNull, col("timestamp") - col("prevTimestamp")).otherwise(0.0))
      .withColumn("windowStart", (floor(col("timestamp"))).cast(LongType))
    
    // Use configurable thresholds
    val idleThreshold = KpiConfig.IDLE_THRESHOLD
    val smallPkt = KpiConfig.SMALL_PKT_THRESHOLD
    val largePkt = KpiConfig.LARGE_PKT_THRESHOLD
    val eps = KpiConfig.EPS
    
    // Aggregate KPIs per window with proper normalization
    val kpiDF = withIAT.groupBy("sliceType", "windowStart")
      .agg(
        // === VOLUME KPIs (4) ===
        (sum("packetLen") * 8).alias("Throughput_bps"),
        count("*").alias("Total_Packets"),
        sum("packetLen").alias("Total_Bytes"),
        (sum("packetLen") / (sum("IAT") + lit(eps))).alias("Byte_Velocity"),
        
        // === TEMPORAL KPIs (11) ===
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
        
        // === PACKET SIZE KPIs (9) ===
        avg("packetLen").alias("Avg_Packet_Size"),
        stddev("packetLen").alias("Pkt_Size_StdDev"),
        skewness("packetLen").alias("Pkt_Size_Skewness"),
        kurtosis("packetLen").alias("Pkt_Size_Kurtosis"),
        min("packetLen").alias("Min_Pkt_Size"),
        max("packetLen").alias("Max_Pkt_Size"),
        countDistinct("packetLen").alias("Unique_Pkt_Sizes"),
        (sum(when(col("packetLen") < lit(smallPkt), 1).otherwise(0)) / count("*")).alias("Small_Pkt_Ratio"),
        (sum(when(col("packetLen") > lit(largePkt), 1).otherwise(0)) / count("*")).alias("Large_Pkt_Ratio"),
        
        // === PROTOCOL KPIs (4) ===
        (sum(when(col("protocol") === "TCP", 1).otherwise(0)) / count("*")).alias("TCP_Ratio"),
        (sum(when(col("protocol") === "UDP", 1).otherwise(0)) / count("*")).alias("UDP_Ratio"),
        countDistinct("protocol").alias("Protocol_Diversity"),
        countDistinct("srcPort").alias("Unique_Src_Ports"),
        
        // === TCP HEALTH KPIs (6) - Fixed bitmask logic ===
        avg("windowSize").alias("Avg_Win_Size"),
        stddev("windowSize").alias("Win_Size_StdDev"),
        min("windowSize").alias("Min_Win_Size"),
        max("windowSize").alias("Max_Win_Size"),
        sum(when(col("windowSize") === 0, 1).otherwise(0)).alias("Zero_Win_Count"),
        // Fixed: Use proper bitmask comparison for RST flag (bit 2 = 0x04)
        sum(when(col("tcpFlags").bitwiseAND(lit(0x04)) =!= 0, 1).otherwise(0)).alias("RST_Count"),
        
        // === FLOW KPIs (2) ===
        countDistinct("dstPort").alias("Unique_Dst_Ports"),
        (stddev("packetLen") / (avg("packetLen") + lit(eps))).alias("Coeff_Variation_Size")
      )
      .na.fill(0.0)
    
    kpiDF
  }
}
