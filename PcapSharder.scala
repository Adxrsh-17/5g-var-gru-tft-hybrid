import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataInputStream, FSDataOutputStream, FileUtil}
import java.net.URI

object PcapSharder {
  val HDFS_URI = "hdfs://namenode:8020"
  val SOURCE_ROOT = "/5g_kpi/raw/pcap" 
  val DEST_ROOT   = "/5g_kpi/sharded" 
  val CHUNK_SIZE = 1024L * 1024L * 1024L   // 1 GB Split Size
  val HEADER_SIZE = 24                     // PCAP Global Header

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", HDFS_URI)
    val fs = FileSystem.get(new URI(HDFS_URI), conf)
    
    println(s"\n?? STARTING SMART SHARDING & COPY PROCESS")
    try {
      val fileSystemIterator = fs.listFiles(new Path(SOURCE_ROOT), true)
      while (fileSystemIterator.hasNext) {
        val fileStatus = fileSystemIterator.next()
        val srcPath = fileStatus.getPath
        
        if (srcPath.getName.endsWith(".pcap")) {
          val sliceName = srcPath.getParent.getName
          val targetSliceDir = new Path(DEST_ROOT + "/" + sliceName)
          if (!fs.exists(targetSliceDir)) fs.mkdirs(targetSliceDir)

          if (fileStatus.getLen > 2147483647L) {
             shardFile(fs, srcPath, targetSliceDir)
          } else {
             val destPath = new Path(targetSliceDir, srcPath.getName)
             if (!fs.exists(destPath)) FileUtil.copy(fs, srcPath, fs, destPath, false, conf)
          }
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      fs.close()
    }
  }

  def shardFile(fs: FileSystem, srcPath: Path, targetDir: Path): Unit = {
    val inStream = fs.open(srcPath)
    val header = new Array[Byte](HEADER_SIZE)
    val buffer = new Array[Byte](8192)
    try {
      inStream.readFully(header)
      var partNum = 1
      var currentBytesWritten = 0L
      var outPath = getOutputPath(targetDir, srcPath.getName, partNum)
      if (fs.exists(outPath)) return

      var outStream = fs.create(outPath)
      outStream.write(header)
      var bytesRead = inStream.read(buffer)
      while (bytesRead != -1) {
        outStream.write(buffer, 0, bytesRead)
        currentBytesWritten += bytesRead
        if (currentBytesWritten >= CHUNK_SIZE) {
          outStream.close()
          partNum += 1
          currentBytesWritten = 0
          outPath = getOutputPath(targetDir, srcPath.getName, partNum)
          outStream = fs.create(outPath)
          outStream.write(header)
        }
        bytesRead = inStream.read(buffer)
      }
      outStream.close()
    } finally {
      inStream.close()
    }
  }

  def getOutputPath(dir: Path, originalName: String, part: Int): Path = {
    val name = originalName.replace(".pcap", "")
    new Path(dir, f"${name}_part$part%03d.pcap")
  }
}
