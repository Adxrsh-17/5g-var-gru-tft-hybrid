// PcapKpiExtractor.scala
// PHASE 2: PCAP INGESTION - CLUSTER OPTIMIZED

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.input.PortableDataStream
import java.io.{DataInputStream, BufferedInputStream}
import java.nio.{ByteBuffer, ByteOrder}

object KpiConfig {
  val MAX_PACKETS: Int = sys.env.getOrElse("KPI_MAX_PACKETS", "999999999").toInt
  val KAFKA_BOOTSTRAP: String = sys.env.getOrElse("KAFKA_BOOTSTRAP", "bd_kafka:9092")
  val KAFKA_TOPIC: String = sys.env.getOrElse("KAFKA_PACKET_TOPIC", "5g-packet-events")
  
  // UPDATED: Defaulting to 172.18.0.4 to match your working configuration
  val HDFS_NAMENODE: String = sys.env.getOrElse("HDFS_NAMENODE", "hdfs://172.18.0.4:8020")
  val HDFS_RAW_PCAP: String = s"$HDFS_NAMENODE/5G_kpi/raw/pcap"
  val MIN_PARTITIONS: Int = sys.env.getOrElse("MIN_PARTITIONS", "200").toInt
}

case class PacketEvent(
  sliceType: String, fileName: String, timestamp: Double, timestampMs: Long,
  packetLen: Int, capturedLen: Int, protocol: String,
  srcIp: String, dstIp: String, srcPort: Int, dstPort: Int, flowId: String,
  ipHeaderLen: Int, tcpFlags: Int, windowSize: Int, seqNumber: Long
) extends Serializable

object PcapDecoder extends Serializable {
  def decodePcapFromStream(pds: PortableDataStream, sliceType: String, fileName: String): Iterator[PacketEvent] = {
    val packets = new scala.collection.mutable.ArrayBuffer[PacketEvent]()
    var inputStream: DataInputStream = null
    try {
      inputStream = new DataInputStream(new BufferedInputStream(pds.open(), 65536))
      val magicNumber = inputStream.readInt()
      val isLittleEndian = (magicNumber == 0xd4c3b2a1 || magicNumber == 0x4d3cb2a1)
      inputStream.skipBytes(20)
      
      var count = 0
      while (inputStream.available() > 16 && count < KpiConfig.MAX_PACKETS) {
        try {
          val tsSec = readInt(inputStream, isLittleEndian)
          val tsUsec = readInt(inputStream, isLittleEndian)
          val capLen = readInt(inputStream, isLittleEndian)
          val origLen = readInt(inputStream, isLittleEndian)
          
          if (capLen > 0 && capLen < 65536) {
            val data = new Array[Byte](capLen)
            inputStream.readFully(data)
            packets += parsePacket(data, sliceType, fileName, tsSec, tsUsec, origLen, capLen)
            count += 1
          } else if (capLen > 0) inputStream.skipBytes(capLen)
        } catch { case _: Exception => }
      }
    } catch { case _: Exception => } 
    finally { if (inputStream != null) try { inputStream.close() } catch { case _: Exception => } }
    packets.iterator
  }
  
  private def readInt(dis: DataInputStream, le: Boolean): Int = {
    val b = new Array[Byte](4)
    dis.readFully(b)
    if (le) ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt
    else ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getInt
  }
  
  private def parsePacket(data: Array[Byte], slice: String, file: String, sec: Int, usec: Int, orig: Int, cap: Int): PacketEvent = {
    var proto = "OTHER"; var src = "0.0.0.0"; var dst = "0.0.0.0"
    var sPort = 0; var dPort = 0; var ipLen = 0; var tcpF = 0; var win = 0; var seq = 0L
    try {
      val ethType = ((data(12) & 0xFF) << 8) | (data(13) & 0xFF)
      val ipOff = if (ethType == 0x0800) 14 else 0
      if (data.length > ipOff + 20) {
        ipLen = (data(ipOff) & 0x0F) * 4
        val protoNum = data(ipOff + 9) & 0xFF
        src = s"${data(ipOff+12)&0xFF}.${data(ipOff+13)&0xFF}.${data(ipOff+14)&0xFF}.${data(ipOff+15)&0xFF}"
        dst = s"${data(ipOff+16)&0xFF}.${data(ipOff+17)&0xFF}.${data(ipOff+18)&0xFF}.${data(ipOff+19)&0xFF}"
        proto = protoNum match { case 6 => "TCP" case 17 => "UDP" case 1 => "ICMP" case _ => "OTHER" }
        val transOff = ipOff + ipLen
        if (data.length > transOff + 4) {
          sPort = ((data(transOff) & 0xFF) << 8) | (data(transOff+1) & 0xFF)
          dPort = ((data(transOff+2) & 0xFF) << 8) | (data(transOff+3) & 0xFF)
          if (proto == "TCP" && data.length > transOff + 16) {
             tcpF = data(transOff+13) & 0xFF
             win = ((data(transOff+14) & 0xFF) << 8) | (data(transOff+15) & 0xFF)
          }
        }
      }
    } catch { case _: Exception => }
    val ts = (sec.toLong & 0xFFFFFFFFL) + ((usec.toLong & 0xFFFFFFFFL) / 1000000.0)
    val tsMs = (sec.toLong * 1000L) + (usec.toLong / 1000L)
    val flow = s"${src}_${dst}_${sPort}_${dPort}_${proto}"
    PacketEvent(slice, file, ts, tsMs, orig, cap, proto, src, dst, sPort, dPort, flow, ipLen, tcpF, win, seq)
  }
}

object PcapKpiExtractor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_PCAP_Extractor_Production")
      .config("spark.hadoop.fs.defaultFS", KpiConfig.HDFS_NAMENODE)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
      
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println(s"DEBUG: KpiConfig.HDFS_NAMENODE used: ${KpiConfig.HDFS_NAMENODE}")

    val globPath = s"${KpiConfig.HDFS_RAW_PCAP}/*/*.pcap"
    val pcapRDD = spark.sparkContext
      .binaryFiles(globPath, minPartitions = KpiConfig.MIN_PARTITIONS)
      .mapPartitions { iterator =>
        iterator.flatMap { case (filePath, stream) =>
          val fileName = filePath.split("/").last
          val sliceType = if (filePath.contains("eMBB")) "eMBB"
                     else if (filePath.contains("URLLC")) "URLLC"
                     else if (filePath.contains("mMTC")) "mMTC"
                     else "UNKNOWN"
          PcapDecoder.decodePcapFromStream(stream, sliceType, fileName)
        }
      }
      
    val packetDF = pcapRDD.toDF()
    
    packetDF
      .selectExpr("CAST(flowId AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", KpiConfig.KAFKA_BOOTSTRAP)
      .option("topic", KpiConfig.KAFKA_TOPIC)
      .option("kafka.compression.type", "snappy")
      .option("kafka.batch.size", "65536")
      .option("kafka.linger.ms", "50")
      .save()
      
    spark.stop()
  }
}