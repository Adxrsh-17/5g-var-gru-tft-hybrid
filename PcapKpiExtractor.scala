// PcapKpiExtractor.scala
// Phase 2: Distributed PCAP Processing with Packet-Level Kafka Streaming
// Architecture: HDFS → Spark (Executor-Side Decoding) → Kafka (Packet Events) → Ready for Streaming KPI

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.input.PortableDataStream
import java.io.{DataInputStream, BufferedInputStream}
import java.nio.{ByteBuffer, ByteOrder}

// ============================================
// CONFIGURATION MODULE
// ============================================
object KpiConfig extends Serializable {
  val IDLE_THRESHOLD: Double = sys.env.getOrElse("KPI_IDLE_THRESHOLD", "0.1").toDouble
  val SMALL_PKT_THRESHOLD: Int = sys.env.getOrElse("KPI_SMALL_PKT", "100").toInt
  val LARGE_PKT_THRESHOLD: Int = sys.env.getOrElse("KPI_LARGE_PKT", "1400").toInt
  val MAX_PACKETS_PER_FILE: Int = sys.env.getOrElse("KPI_MAX_PACKETS", "100000").toInt
  val EPS: Double = 1e-6  // Epsilon for numerical stability
  
  // Kafka Configuration
  val KAFKA_BOOTSTRAP_SERVERS: String = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
  val KAFKA_PACKET_TOPIC: String = sys.env.getOrElse("KAFKA_PACKET_TOPIC", "5g-packet-events")
  val KAFKA_BATCH_SIZE: Int = sys.env.getOrElse("KAFKA_BATCH_SIZE", "1000").toInt
  
  // HDFS Paths
  val HDFS_BASE: String = "hdfs://namenode:8020/5G_kpi"
  val HDFS_RAW_PCAP: String = s"$HDFS_BASE/raw/pcap"
  val HDFS_CHECKPOINT: String = s"$HDFS_BASE/checkpoints"
}

// ============================================
// PACKET EVENT SCHEMA (Kafka Payload)
// ============================================
case class PacketEvent(
  sliceType: String,        // eMBB / URLLC / mMTC
  fileName: String,
  timestamp: Double,        // Unix timestamp (seconds.microseconds)
  timestampMs: Long,        // Milliseconds for event-time
  packetLen: Int,
  capturedLen: Int,
  protocol: String,         // TCP / UDP / ICMP / OTHER
  srcIp: String,
  dstIp: String,
  srcPort: Int,
  dstPort: Int,
  flowId: String,           // Flow identifier: srcIp_dstIp_srcPort_dstPort_protocol
  ipHeaderLen: Int,
  tcpFlags: Int,
  windowSize: Int,
  seqNumber: Long
) extends Serializable

// ============================================
// PCAP DECODER MODULE (Executor-Side)
// ============================================
object PcapDecoder extends Serializable {
  
  /**
   * Decodes PCAP file from PortableDataStream (runs on executors)
   * Returns iterator of PacketEvent objects
   */
  def decodePcapFromStream(
    pds: PortableDataStream, 
    sliceType: String, 
    fileName: String, 
    maxPackets: Int
  ): Iterator[PacketEvent] = {
    
    val packets = scala.collection.mutable.ArrayBuffer[PacketEvent]()
    var inputStream: DataInputStream = null
    
    try {
      inputStream = new DataInputStream(new BufferedInputStream(pds.open(), 65536))
      
      // Read Global Header (24 bytes)
      val magicNumber = inputStream.readInt()
      val isLittleEndian = (magicNumber == 0xd4c3b2a1 || magicNumber == 0x4d3cb2a1)
      
      // Skip rest of global header (20 bytes)
      inputStream.skipBytes(20)
      
      var packetCount = 0
      
      // Read packets
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
      case _: java.io.EOFException => // End of file - normal
      case e: Exception => 
        // Log error but continue (distributed processing should be resilient)
        System.err.println(s"Error decoding $fileName: ${e.getMessage}")
    } finally {
      if (inputStream != null) inputStream.close()
      // PortableDataStream closes automatically when its InputStream is closed
    }
    
    packets.iterator
  }
  
  /**
   * Reads 4-byte integer with endianness handling
   */
  private def readInt(dis: DataInputStream, littleEndian: Boolean): Int = {
    val bytes = new Array[Byte](4)
    dis.readFully(bytes)
    if (littleEndian) {
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt
    } else {
      ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt
    }
  }
  
  /**
   * Parses packet data to extract headers and create PacketEvent
   */
  private def parsePacket(
    data: Array[Byte], 
    sliceType: String, 
    fileName: String,
    tsSec: Int, 
    tsUsec: Int, 
    origLen: Int, 
    capLen: Int
  ): PacketEvent = {
    
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
      case _: Exception => // Keep defaults on parse failure
    }
    
    // Create timestamp values
    val timestamp = (tsSec.toLong & 0xFFFFFFFFL) + ((tsUsec.toLong & 0xFFFFFFFFL) / 1000000.0)
    val timestampMs = ((tsSec.toLong & 0xFFFFFFFFL) * 1000L) + ((tsUsec.toLong & 0xFFFFFFFFL) / 1000L)
    
    // Create flow ID for proper flow-based KPI computation
    val flowId = s"${srcIp}_${dstIp}_${srcPort}_${dstPort}_${protocol}"
    
    PacketEvent(
      sliceType = sliceType,
      fileName = fileName,
      timestamp = timestamp,
      timestampMs = timestampMs,
      packetLen = origLen,
      capturedLen = capLen,
      protocol = protocol,
      srcIp = srcIp,
      dstIp = dstIp,
      srcPort = srcPort,
      dstPort = dstPort,
      flowId = flowId,
      ipHeaderLen = ipHeaderLen,
      tcpFlags = tcpFlags,
      windowSize = windowSize,
      seqNumber = seqNumber
    )
  }
}

// ============================================
// KAFKA PRODUCER MODULE
// ============================================
object KafkaPacketProducer extends Serializable {
  
  /**
   * Publishes packet events to Kafka in batches
   * Each message is a single packet event (event-driven architecture)
   */
  def publishPacketsToKafka(packetDF: DataFrame, spark: SparkSession): Long = {
    import spark.implicits._
    
    val kafkaBootstrap = KpiConfig.KAFKA_BOOTSTRAP_SERVERS
    val kafkaTopic = KpiConfig.KAFKA_PACKET_TOPIC
    
    println(s"\n📤 Publishing packets to Kafka...")
    println(s"   Topic: $kafkaTopic")
    println(s"   Bootstrap: $kafkaBootstrap")
    
    try {
      // Convert PacketEvent to JSON and publish to Kafka
      // Key = flowId for proper partitioning by flow
      val kafkaDF = packetDF
        .selectExpr("flowId AS key", "to_json(struct(*)) AS value")
      
      kafkaDF.write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrap)
        .option("topic", kafkaTopic)
        .option("kafka.compression.type", "snappy")
        .save()
      
      val count = packetDF.count()
      println(s"   ✅ Published $count packet events to Kafka!")
      count
      
    } catch {
      case e: Exception =>
        println(s"   ❌ Kafka publish error: ${e.getMessage}")
        println(s"   Stack trace: ${e.printStackTrace()}")
        0L
    }
  }
}

// Standalone decode function to avoid closure serialization issues
def decodePcapFromStreamLocal(
  pds: PortableDataStream, 
  sliceType: String, 
  fileName: String, 
  maxPackets: Int
): Iterator[PacketEvent] = PcapDecoder.decodePcapFromStream(pds, sliceType, fileName, maxPackets)

// ============================================
// MAIN APPLICATION
// ============================================
object PcapKpiExtractor extends Serializable {

  def main(args: Array[String]): Unit = {
    
    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("5G_PCAP_Packet_Event_Extractor")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    printBanner()
    printConfiguration()
    
    val hdfsBasePath = KpiConfig.HDFS_RAW_PCAP
    
    // ========================================
    // STEP 1: Discover PCAP Files from HDFS
    // ========================================
    println("\n" + "="*60)
    println("STEP 1: Discovering PCAP Files from HDFS")
    println("="*60)
    
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val sliceDirs = Seq(
      ("eMBB", s"$hdfsBasePath/emBB"),
      ("URLLC", s"$hdfsBasePath/urllc"),
      ("mMTC", s"$hdfsBasePath/mmtc")
    )
    
    // Collect file paths with their slice types
    val pcapFilesWithSlice = sliceDirs.flatMap { case (sliceType, dirPath) =>
      try {
        val path = new Path(dirPath)
        if (fs.exists(path)) {
          val files = fs.listStatus(path).filter(_.getPath.getName.endsWith(".pcap"))
          files.map(f => (f.getPath.toString, sliceType))
        } else {
          println(s"   ⚠️  Directory not found: $dirPath")
          Seq.empty
        }
      } catch {
        case e: Exception =>
          println(s"   ❌ Error accessing $dirPath: ${e.getMessage}")
          Seq.empty
      }
    }
    
    if (pcapFilesWithSlice.isEmpty) {
      println("\n❌ No PCAP files found! Exiting...")
      spark.stop()
      return
    }
    
    println(s"\n📂 Found ${pcapFilesWithSlice.length} PCAP files")
    pcapFilesWithSlice.groupBy(_._2).foreach { case (slice, files) =>
      println(s"   $slice: ${files.length} files")
    }
    
    // Create broadcast variable for slice type lookup
    val sliceTypeMap = spark.sparkContext.broadcast(pcapFilesWithSlice.toMap)
    val maxPackets = KpiConfig.MAX_PACKETS_PER_FILE
    
    // ========================================
    // STEP 2: Distributed PCAP Decoding (Executor-Side)
    // ========================================
    println("\n" + "="*60)
    println("STEP 2: Distributed PCAP Decoding (Executor-Side)")
    println("="*60)
    println("📦 Architecture: binaryFiles + flatMap → Executor decoding")
    
    val pcapPaths = pcapFilesWithSlice.map(_._1).mkString(",")
    
    // KEY: This runs on EXECUTORS, not Driver
    // Use mapPartitions with explicit local function reference to avoid serialization issues
    val packetRDD = spark.sparkContext
      .binaryFiles(pcapPaths, minPartitions = pcapFilesWithSlice.length)
      .mapPartitions { iter =>
        val localSliceMap = sliceTypeMap.value
        val localMaxPackets = maxPackets
        iter.flatMap { case (filePath, portableDataStream) =>
          val sliceType = localSliceMap.getOrElse(filePath, "UNKNOWN")
          val fileName = filePath.split("/").last
          PcapDecoder.decodePcapFromStream(portableDataStream, sliceType, fileName, localMaxPackets)
        }
      }
    
    // Convert RDD to DataFrame
    val packetDF = packetRDD.toDF()
    
    val packetCount = packetDF.count()
    println(s"\n✅ Decoded $packetCount packets (distributed)")
    
    if (packetCount == 0) {
      println("\n❌ No packets decoded! Check PCAP files. Exiting...")
      spark.stop()
      return
    }
    
    // Show schema
    println("\n📋 Packet Event Schema:")
    packetDF.printSchema()
    
    // Show distribution
    println("\n📊 Packets by Slice Type:")
    packetDF.groupBy("sliceType").count().orderBy(desc("count")).show()
    
    println("\n📊 Packets by Protocol:")
    packetDF.groupBy("protocol").count().orderBy(desc("count")).show()
    
    // Show sample packets
    println("\n📋 Sample Packet Events:")
    packetDF.select(
      "sliceType", "timestamp", "protocol", "packetLen", 
      "srcIp", "dstIp", "srcPort", "dstPort", "flowId"
    ).show(10, truncate = false)
    
    // ========================================
    // STEP 3: Publish Packets to Kafka
    // ========================================
    println("\n" + "="*60)
    println("STEP 3: Publishing Packet Events to Kafka")
    println("="*60)
    
    val publishedCount = KafkaPacketProducer.publishPacketsToKafka(packetDF, spark)
    
    // ========================================
    // STEP 4: Summary and Architecture Info
    // ========================================
    println("\n" + "="*60)
    println("✅ PHASE 2 COMPLETE: PACKET-LEVEL EVENT STREAMING")
    println("="*60)
    
    println("\n📊 Processing Summary:")
    println(s"   Files Processed:      ${pcapFilesWithSlice.length}")
    println(s"   Packets Decoded:      $packetCount")
    println(s"   Packets Published:    $publishedCount")
    println(s"   Kafka Topic:          ${KpiConfig.KAFKA_PACKET_TOPIC}")
    
    println("\n🏗️  Architecture:")
    println("   HDFS (raw PCAP) → Spark Executors (decode) → Kafka (packet events)")
    println("   ✓ Distributed decoding (no driver bottleneck)")
    println("   ✓ Event-driven architecture (one message per packet)")
    println("   ✓ Flow-based partitioning (flowId as Kafka key)")
    
    println("\n🚀 Next Phase:")
    println("   Phase 3: Spark Structured Streaming → KPI Computation")
    println("   - Read from Kafka (event-time semantics)")
    println("   - Compute 36 KPIs per flow per window")
    println("   - Write to HDFS (checkpointed, exactly-once)")
    
    // Cleanup
    sliceTypeMap.unpersist()
    spark.stop()
  }
  
  // ========================================
  // UTILITY FUNCTIONS
  // ========================================
  
  private def printBanner(): Unit = {
    println("\n" + "="*60)
    println("   5G PCAP-TO-PACKET-EVENT PIPELINE")
    println("   Phase 2: Distributed PCAP Decoding + Kafka Streaming")
    println("="*60)
  }
  
  private def printConfiguration(): Unit = {
    println(s"\n⚙️  Configuration:")
    println(s"   IDLE_THRESHOLD:       ${KpiConfig.IDLE_THRESHOLD}s")
    println(s"   SMALL_PKT:            ${KpiConfig.SMALL_PKT_THRESHOLD} bytes")
    println(s"   LARGE_PKT:            ${KpiConfig.LARGE_PKT_THRESHOLD} bytes")
    println(s"   MAX_PACKETS/FILE:     ${KpiConfig.MAX_PACKETS_PER_FILE}")
    println(s"   EPS:                  ${KpiConfig.EPS}")
    println(s"   KAFKA_BOOTSTRAP:      ${KpiConfig.KAFKA_BOOTSTRAP_SERVERS}")
    println(s"   KAFKA_PACKET_TOPIC:   ${KpiConfig.KAFKA_PACKET_TOPIC}")
    println(s"   KAFKA_BATCH_SIZE:     ${KpiConfig.KAFKA_BATCH_SIZE}")
  }
}
