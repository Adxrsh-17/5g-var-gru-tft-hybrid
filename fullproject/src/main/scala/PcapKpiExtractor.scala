// PcapKpiExtractor.scala
// ARCHITECTURE: STRICT FILE-BY-FILE PROCESSING (1GB Steps)

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{Trigger, OutputMode}
import org.apache.spark.sql.types._
import java.io.{DataInputStream, ByteArrayInputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Timestamp

object KpiConfig {
  val HDFS_NAMENODE: String = sys.env.getOrElse("HDFS_NAMENODE", "hdfs://namenode:8020")
  val BASE_PATH: String = s"$HDFS_NAMENODE/5g_kpi"
  
  // INPUT: The parent folder containing slice subfolders (eMBB, URLLC, mMTC)
  val SHARDED_PCAP_GLOB: String = s"$BASE_PATH/sharded" 
  
  // OUTPUT: Where the calculated KPIs go
  val OUTPUT_PATH: String = s"$BASE_PATH/processed"
  val CHECKPOINT_PATH: String = s"$BASE_PATH/checkpoints/file_stream"
  
  // CRITICAL: Process exactly ONE 1GB file at a time
  val MAX_FILES_PER_TRIGGER: Int = 1  
  val WINDOW_DURATION: String = "1 second"
  val WATERMARK_DELAY: String = "10 seconds"
}

case class PacketEvent(
  sliceType: String, fileName: String, timestamp: Double, 
  packetLen: Int, protocol: String, srcPort: Int, dstPort: Int, 
  tcpFlags: Int, windowSize: Int, flowId: String, eventTime: Timestamp
)

object PcapDecoder extends Serializable {
  def decodeBytes(content: Array[Byte], filePath: String): Iterator[PacketEvent] = {
    val packets = new scala.collection.mutable.ArrayBuffer[PacketEvent]()
    
    // 1. IDENTIFY SLICE FROM FOLDER NAME
    val sliceType = if (filePath.contains("/eMBB/") || filePath.contains("eMBB")) "eMBB" 
               else if (filePath.contains("/URLLC/") || filePath.contains("URLLC")) "URLLC" 
               else if (filePath.contains("/mMTC/") || filePath.contains("mMTC")) "mMTC" 
               else "UNKNOWN"
               
    val fileName = filePath.split("/").last
    val inputStream = new DataInputStream(new ByteArrayInputStream(content))

    try {
      // Pcap Header Parsing
      val magicNumber = inputStream.readInt()
      val isLittleEndian = (magicNumber == 0xd4c3b2a1 || magicNumber == 0x4d3cb2a1)
      inputStream.skipBytes(20) 
      
      var count = 0
      val SAFETY_LIMIT = 2000000 
      
      while (inputStream.available() > 16 && count < SAFETY_LIMIT) {
        try {
           val tsSec = readInt(inputStream, isLittleEndian)
           val tsUsec = readInt(inputStream, isLittleEndian)
           val capLen = readInt(inputStream, isLittleEndian)
           val origLen = readInt(inputStream, isLittleEndian) 
           
           if (capLen > 0 && capLen <= 65536) {
             val packetData = new Array[Byte](capLen)
             inputStream.readFully(packetData)
             packets += parsePacket(packetData, sliceType, fileName, tsSec, tsUsec, capLen)
             count += 1
           } else if (capLen > 0) inputStream.skipBytes(capLen)
        } catch { case _: Exception => }
      }
    } catch { case _: Exception => }
    packets.iterator
  }

  private def readInt(dis: DataInputStream, le: Boolean): Int = {
    val b = new Array[Byte](4); dis.readFully(b)
    if (le) ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt
    else ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getInt
  }

  private def parsePacket(data: Array[Byte], slice: String, file: String, sec: Int, usec: Int, len: Int): PacketEvent = {
    var proto="OTHER"; var srcP=0; var dstP=0; var flags=0; var win=0
    var srcIp="0.0.0.0"; var dstIp="0.0.0.0"

    try {
      val ethType = if (data.length > 13) ((data(12) & 0xFF) << 8) | (data(13) & 0xFF) else 0
      val ipOff = if (ethType == 0x0800) 14 else 0
      if (data.length > ipOff + 20) {
        val protoNum = data(ipOff + 9) & 0xFF
        srcIp = s"${data(ipOff+12)&0xFF}.${data(ipOff+13)&0xFF}.${data(ipOff+14)&0xFF}.${data(ipOff+15)&0xFF}"
        dstIp = s"${data(ipOff+16)&0xFF}.${data(ipOff+17)&0xFF}.${data(ipOff+18)&0xFF}.${data(ipOff+19)&0xFF}"
        proto = protoNum match { case 6 => "TCP" case 17 => "UDP" case 1 => "ICMP" case _ => "OTHER" }
        
        val transOff = ipOff + (data(ipOff)&0x0F)*4
        if (data.length > transOff + 4) {
          srcP = ((data(transOff)&0xFF)<<8) | (data(transOff+1)&0xFF)
          dstP = ((data(transOff+2)&0xFF)<<8) | (data(transOff+3)&0xFF)
          if (proto == "TCP" && data.length > transOff + 16) {
            flags = data(transOff+13) & 0xFF
            win = ((data(transOff+14)&0xFF)<<8) | (data(transOff+15)&0xFF)
          }
        }
      }
    } catch { case _: Exception => }

    val ts = sec.toDouble + (usec.toDouble / 1000000.0)
    val flow = s"${srcIp}_${dstIp}_${srcP}_${dstP}_${proto}"
    val eventTime = new Timestamp((ts * 1000).toLong) 
    PacketEvent(slice, file, ts, len, proto, srcP, dstP, flags, win, flow, eventTime)
  }
}

object PcapKpiExtractor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_KPI_One_By_One")
      .master("local[*]")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("\n" + "="*60)
    println(s"?? STARTING SLICE-WISE KPI EXTRACTION")
    println(s"?? Input: ${KpiConfig.SHARDED_PCAP_GLOB}")
    println(s"?? Output: ${KpiConfig.OUTPUT_PATH}")
    println("="*60)

    // DEFINE SCHEMA MANUALLY
    val binarySchema = new StructType()
      .add("path", StringType, false)
      .add("modificationTime", TimestampType, false)
      .add("length", LongType, false)
      .add("content", BinaryType, true)

    // 1. INGEST
    val rawStream = spark.readStream
      .format("binaryFile")
      .schema(binarySchema)
      .option("pathGlobFilter", "*.pcap")
      .option("recursiveFileLookup", "true") 
      .option("maxFilesPerTrigger", KpiConfig.MAX_FILES_PER_TRIGGER) 
      .load(KpiConfig.SHARDED_PCAP_GLOB)

    // 2. DECODE
    val packetStream = rawStream.select("path", "content").as[(String, Array[Byte])].flatMap { 
      case (path, content) => PcapDecoder.decodeBytes(content, path)
    }

    // 3. AGGREGATE
    val kpiStream = packetStream
      .withWatermark("eventTime", KpiConfig.WATERMARK_DELAY)
      .groupBy(
        col("sliceType"),
        window(col("eventTime"), KpiConfig.WINDOW_DURATION)
      )
      .agg(
        (sum("packetLen") * 8).alias("Throughput_bps"),
        (avg("packetLen")).alias("Avg_Packet_Size"),
        (max("packetLen")).alias("Max_Packet_Size"),
        (min("packetLen")).alias("Min_Packet_Size"),
        (stddev("packetLen")).alias("Jitter_Variance"),
        count("*").alias("Total_Packets"),
        (sum(when(col("protocol") === "TCP", 1).otherwise(0))).alias("TCP_Packets"),
        (sum(when(col("protocol") === "UDP", 1).otherwise(0))).alias("UDP_Packets"),
        approx_count_distinct("flowId").alias("Active_Flows"),
        (sum(when(expr("cast(tcpFlags as int) & 2") === 2, 1).otherwise(0))).alias("TCP_Syn_Count"),
        (sum(when(expr("cast(tcpFlags as int) & 1") === 1, 1).otherwise(0))).alias("TCP_Fin_Count")
      )
      .select(
        col("sliceType"), 
        col("window.start").alias("window_start"), 
        col("window.end").alias("window_end"), 
        col("*")
      ).drop("window")

    // 4. SINK
    val query = kpiStream.writeStream
      .format("parquet")
      .option("path", KpiConfig.OUTPUT_PATH)
      .option("checkpointLocation", KpiConfig.CHECKPOINT_PATH)
      .outputMode(OutputMode.Append())
      .partitionBy("sliceType")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()

    println(s"? Pipeline Running. Processing files one by one...")
    query.awaitTermination()
  }
}
