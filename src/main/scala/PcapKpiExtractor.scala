// PcapKpiExtractor.scala
// ARCHITECTURE: BATCH PROCESSING WITH MANUAL CHUNK-BASED SPLIT-FILE CONCATENATION
// Split PCAP files (created by Unix `split`) are read sequentially with a leftover
// buffer so packets that span 1GB file boundaries are correctly reconstructed.
// Results are written in 1M-packet batches to control memory usage.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Timestamp
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration

object KpiConfig {
  val HDFS_NAMENODE: String = sys.env.getOrElse("HDFS_NAMENODE", "hdfs://namenode:8020")
  val BASE_PATH: String = s"$HDFS_NAMENODE/5g_kpi"
  val SHARDED_PCAP_GLOB: String = s"$BASE_PATH/sharded"
  val OUTPUT_PATH: String = s"$BASE_PATH/processed"
  val WINDOW_DURATION: String = "1 second"
  val BATCH_SIZE: Int = 1000000 // 1M packets per batch to control memory
}

case class PacketEvent(
  sliceType: String, fileName: String, timestamp: Double,
  packetLen: Int, protocol: String, srcPort: Int, dstPort: Int,
  tcpFlags: Int, windowSize: Int, flowId: String, eventTime: Timestamp
)

object PcapDecoder extends Serializable {
  private val PCAP_MAGIC_LE    = 0xd4c3b2a1
  private val PCAP_MAGIC_BE    = 0xa1b2c3d4.toInt
  private val PCAP_MAGIC_NS_LE = 0x4d3cb2a1
  private val PCAP_MAGIC_NS_BE = 0xa1b23c4d.toInt
  private val CHUNK_SIZE       = 8 * 1024 * 1024  // 8 MB read chunk

  /**
   * Decode a group of split PCAP files (or a single standalone file).
   * Files are read sequentially with a leftover buffer to handle packets
   * that span the 1GB split boundary between consecutive files.
   *
   * Calls `onBatch` with each batch of up to BATCH_SIZE packets so the
   * caller can aggregate and write without holding everything in memory.
   *
   * Returns (totalPackets, totalRawBytesDecoded).
   */
  def decodeSplitGroup(
    sortedPaths: Array[String],
    sliceType: String,
    label: String,
    batchSize: Int,
    onBatch: Seq[PacketEvent] => Unit
  ): (Long, Long) = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://namenode:8020")
    val fs = FileSystem.get(hadoopConf)

    var isLE = true
    var headerRead = false
    var leftover = Array.empty[Byte]       // bytes from end of previous chunk/file
    var totalPackets = 0L
    var totalBytes   = 0L
    val batch = new scala.collection.mutable.ArrayBuffer[PacketEvent](Math.min(batchSize, 100000))

    for ((pathStr, fileIdx) <- sortedPaths.zipWithIndex) {
      val path = new Path(pathStr)
      val fileLen = fs.getFileStatus(path).getLen
      val fis = fs.open(path)
      var filePos = 0L

      try {
        // First file only: read the 24-byte PCAP global header
        if (!headerRead) {
          val hdr = new Array[Byte](24)
          fis.readFully(hdr)
          filePos = 24

          // Read magic as LE: for a LE PCAP file (disk: d4 c3 b2 a1) getIntLE returns 0xa1b2c3d4
          val magic = getIntLE(hdr, 0)
          isLE = magic match {
            case 0xa1b2c3d4 => true    // LE standard (disk: d4 c3 b2 a1)
            case 0xa1b23c4d => true    // LE nanosecond (disk: 4d 3c b2 a1)
            case _          => false   // BE standard/nanosecond
          }
          headerRead = true
          System.err.println(s"[HEADER] $sliceType/$label: magic=0x${Integer.toHexString(magic)}, LE=$isLE")
        }

        // Read the file in CHUNK_SIZE chunks
        while (filePos < fileLen) {
          val remaining = Math.min(CHUNK_SIZE.toLong, fileLen - filePos).toInt
          val chunk = new Array[Byte](remaining)

          // readFully guarantees we get all requested bytes
          fis.readFully(chunk)
          filePos += remaining

          // Combine leftover from previous chunk/file with current chunk
          val data: Array[Byte] = if (leftover.nonEmpty) {
            val combined = new Array[Byte](leftover.length + chunk.length)
            System.arraycopy(leftover, 0, combined, 0, leftover.length)
            System.arraycopy(chunk, 0, combined, leftover.length, chunk.length)
            leftover = Array.empty[Byte]
            combined
          } else {
            chunk
          }

          // Parse complete packets from `data`
          var off = 0
          var chunkDone = false
          var skipCount = 0
          while (off + 16 <= data.length && !chunkDone) {
            val capLen = if (isLE) getIntLE(data, off + 8) else getIntBE(data, off + 8)

            if (capLen < 0 || capLen > 65536) {
              // Corrupt data — skip 1 byte and try to re-sync (limit to 65536 skips)
              skipCount += 1
              if (skipCount <= 3) {
                System.err.println(s"[WARN] $sliceType/$label: bad capLen=$capLen at totalPkt=$totalPackets fileIdx=$fileIdx, skipping byte ($skipCount)")
              }
              if (skipCount > 65536) {
                System.err.println(s"[ERROR] $sliceType/$label: exceeded skip limit at totalPkt=$totalPackets, abandoning chunk")
                chunkDone = true
              }
              off += 1
            } else if (off + 16 + capLen > data.length) {
              // Incomplete packet at end of chunk → save as leftover
              chunkDone = true
            } else {
              // Complete packet — extract fields
              val tsSec  = if (isLE) getIntLE(data, off)     else getIntBE(data, off)
              val tsUsec = if (isLE) getIntLE(data, off + 4) else getIntBE(data, off + 4)
              val pktData = new Array[Byte](capLen)
              System.arraycopy(data, off + 16, pktData, 0, capLen)

              batch += parsePacket(pktData, sliceType, label, tsSec, tsUsec, capLen)
              totalPackets += 1
              totalBytes += 16 + capLen
              off += 16 + capLen
              skipCount = 0  // reset skip counter on successful packet

              // Flush batch when full
              if (batch.size >= batchSize) {
                onBatch(batch.toSeq)
                batch.clear()
              }
            }
          }

          // Save remaining bytes as leftover for next chunk/file
          if (off < data.length) {
            leftover = new Array[Byte](data.length - off)
            System.arraycopy(data, off, leftover, 0, leftover.length)
          }
        }
      } finally {
        fis.close()
      }

      System.err.println(s"[FILE] $sliceType/$label: finished file ${fileIdx + 1}/${sortedPaths.length} (${path.getName}), totalPkts=$totalPackets, leftover=${leftover.length} bytes")
    }

    // Flush any remaining packets in the last batch
    if (batch.nonEmpty) {
      onBatch(batch.toSeq)
      batch.clear()
    }

    (totalPackets, totalBytes)
  }

  // --- helper: little-endian 32-bit int from byte array ---
  private def getIntLE(arr: Array[Byte], off: Int): Int =
    (arr(off) & 0xFF) | ((arr(off+1) & 0xFF) << 8) |
    ((arr(off+2) & 0xFF) << 16) | ((arr(off+3) & 0xFF) << 24)

  // --- helper: big-endian 32-bit int from byte array ---
  private def getIntBE(arr: Array[Byte], off: Int): Int =
    ((arr(off) & 0xFF) << 24) | ((arr(off+1) & 0xFF) << 16) |
    ((arr(off+2) & 0xFF) << 8) | (arr(off+3) & 0xFF)

  private def parsePacket(data: Array[Byte], slice: String, file: String, sec: Int, usec: Int, len: Int): PacketEvent = {
    var proto = "OTHER"; var srcP = 0; var dstP = 0; var flags = 0; var win = 0
    var srcIp = "0.0.0.0"; var dstIp = "0.0.0.0"

    try {
      val ethType = if (data.length > 13) ((data(12) & 0xFF) << 8) | (data(13) & 0xFF) else 0
      val ipOff = if (ethType == 0x0800) 14 else 0
      if (data.length > ipOff + 20) {
        val protoNum = data(ipOff + 9) & 0xFF
        srcIp = s"${data(ipOff+12)&0xFF}.${data(ipOff+13)&0xFF}.${data(ipOff+14)&0xFF}.${data(ipOff+15)&0xFF}"
        dstIp = s"${data(ipOff+16)&0xFF}.${data(ipOff+17)&0xFF}.${data(ipOff+18)&0xFF}.${data(ipOff+19)&0xFF}"
        proto = protoNum match { case 6 => "TCP"; case 17 => "UDP"; case 1 => "ICMP"; case _ => "OTHER" }

        val transOff = ipOff + (data(ipOff) & 0x0F) * 4
        if (data.length > transOff + 4) {
          srcP = ((data(transOff) & 0xFF) << 8) | (data(transOff + 1) & 0xFF)
          dstP = ((data(transOff + 2) & 0xFF) << 8) | (data(transOff + 3) & 0xFF)
          if (proto == "TCP" && data.length > transOff + 16) {
            flags = data(transOff + 13) & 0xFF
            win = ((data(transOff + 14) & 0xFF) << 8) | (data(transOff + 15) & 0xFF)
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

  private def buildKpiAndWrite(spark: SparkSession, packetBatch: Seq[PacketEvent], outputPath: String): Long = {
    import spark.implicits._
    val packetDF = spark.createDataFrame(spark.sparkContext.parallelize(packetBatch, 4))

    val kpiDF = packetDF
      .groupBy(col("sliceType"), window(col("eventTime"), KpiConfig.WINDOW_DURATION))
      .agg(
        (sum("packetLen") * 8).alias("Throughput_bps"),
        avg("packetLen").alias("Avg_Packet_Size"),
        max("packetLen").alias("Max_Packet_Size"),
        min("packetLen").alias("Min_Packet_Size"),
        stddev("packetLen").alias("Jitter_Variance"),
        count("*").alias("Total_Packets"),
        sum(when(col("protocol") === "TCP", 1).otherwise(0)).alias("TCP_Packets"),
        sum(when(col("protocol") === "UDP", 1).otherwise(0)).alias("UDP_Packets"),
        approx_count_distinct("flowId").alias("Active_Flows"),
        sum(when(expr("cast(tcpFlags as int) & 2") === 2, 1).otherwise(0)).alias("TCP_Syn_Count"),
        sum(when(expr("cast(tcpFlags as int) & 1") === 1, 1).otherwise(0)).alias("TCP_Fin_Count")
      )
      .select(
        col("sliceType"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("Throughput_bps"), col("Avg_Packet_Size"),
        col("Max_Packet_Size"), col("Min_Packet_Size"),
        col("Jitter_Variance"), col("Total_Packets"),
        col("TCP_Packets"), col("UDP_Packets"),
        col("Active_Flows"), col("TCP_Syn_Count"), col("TCP_Fin_Count")
      )

    val windowCount = kpiDF.count()
    kpiDF.write.mode("append").partitionBy("sliceType").parquet(outputPath)
    windowCount
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_KPI_Batch_ChunkConcat")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "12")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://namenode:8020")
    val fs = FileSystem.get(hadoopConf)

    val sliceDirs = Array("URLLC", "eMBB", "mMTC")
    val totalBytesProcessed   = scala.collection.mutable.Map[String, Long]("URLLC" -> 0L, "eMBB" -> 0L, "mMTC" -> 0L)
    val totalPacketsProcessed = scala.collection.mutable.Map[String, Long]("URLLC" -> 0L, "eMBB" -> 0L, "mMTC" -> 0L)
    val totalWindowsWritten   = scala.collection.mutable.Map[String, Long]("URLLC" -> 0L, "eMBB" -> 0L, "mMTC" -> 0L)

    println("\n" + "=" * 60)
    println("STARTING BATCH KPI EXTRACTION (CHUNK-BASED CONCATENATION)")
    println(s"Input : ${KpiConfig.SHARDED_PCAP_GLOB}")
    println(s"Output: ${KpiConfig.OUTPUT_PATH}")
    println(s"Batch : ${KpiConfig.BATCH_SIZE} packets per batch")
    println("=" * 60)

    for (sliceDir <- sliceDirs) {
      val slicePath = new Path(s"${KpiConfig.SHARDED_PCAP_GLOB}/$sliceDir")
      if (!fs.exists(slicePath)) {
        println(s"\n[$sliceDir] Directory not found: $slicePath")
      } else {
        val allFiles = fs.listStatus(slicePath)
          .filter(_.getPath.getName.endsWith(".pcap"))
          .sortBy(_.getPath.getName)
        val totalMB = allFiles.map(_.getLen).sum / (1024 * 1024)
        println(s"\n[$sliceDir] Found ${allFiles.length} PCAP files ($totalMB MB total)")

        // Group files: detect _partNNN split pattern
        val splitRegex = """(.+)_part(\d+)\.pcap""".r
        val groups: Map[String, Array[FileStatus]] = allFiles.groupBy { f =>
          f.getPath.getName match {
            case splitRegex(prefix, _) => prefix
            case name                  => name
          }
        }

        for ((groupName, groupFiles) <- groups.toSeq.sortBy(_._1)) {
          val sorted = groupFiles.sortBy(_.getPath.getName)
          val groupBytes = sorted.map(_.getLen).sum
          val paths = sorted.map(_.getPath.toString)
          val isSplitGroup = sorted.length > 1

          if (isSplitGroup) {
            println(s"\n  [$sliceDir] SPLIT GROUP: $groupName (${sorted.length} parts, ${groupBytes / (1024*1024)} MB)")
          } else {
            println(s"  [$sliceDir] File: ${sorted(0).getPath.getName} (${groupBytes / (1024*1024)} MB)")
          }

          val t0 = System.currentTimeMillis()
          var groupWindows = 0L
          var batchNum = 0

          try {
            val (groupPackets, groupDecoded) = PcapDecoder.decodeSplitGroup(
              paths, sliceDir, groupName, KpiConfig.BATCH_SIZE,
              (packetBatch: Seq[PacketEvent]) => {
                val bt = System.currentTimeMillis()
                val windows = buildKpiAndWrite(spark, packetBatch, KpiConfig.OUTPUT_PATH)
                groupWindows += windows
                val secs = (System.currentTimeMillis() - bt) / 1000.0
                println(f"    Batch $batchNum%d: ${packetBatch.size}%,d pkts -> $windows%,d windows (${secs}%.1fs)")
                batchNum += 1
              }
            )

            totalBytesProcessed(sliceDir)   += groupBytes
            totalPacketsProcessed(sliceDir) += groupPackets
            totalWindowsWritten(sliceDir)   += groupWindows

            val gb = groupBytes / (1024.0 * 1024.0 * 1024.0)
            val totalSecs = (System.currentTimeMillis() - t0) / 1000.0
            println(f"  [$sliceDir] COMPLETE: $groupName -> $groupPackets%,d packets, $groupWindows%,d windows ($gb%.2f GB, ${totalSecs}%.1fs)")
          } catch {
            case e: Exception =>
              println(s"  [$sliceDir] ERROR on $groupName: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
    }

    // FINAL SUMMARY
    println("\n" + "=" * 60)
    println("PROCESSING COMPLETE - FINAL SUMMARY")
    println("=" * 60)
    for (slice <- sliceDirs) {
      val gb = totalBytesProcessed(slice) / (1024.0 * 1024.0 * 1024.0)
      println(f"  $slice%-6s: $gb%.2f GB streamed, ${totalPacketsProcessed(slice)}%,d packets, ${totalWindowsWritten(slice)}%,d KPI windows")
    }
    val totalGB = totalBytesProcessed.values.sum / (1024.0 * 1024.0 * 1024.0)
    println(f"  TOTAL : $totalGB%.2f GB processed")
    println("=" * 60)

    spark.stop()
  }
}