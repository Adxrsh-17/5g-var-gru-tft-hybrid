// VerifyRead.scala
import org.apache.spark.sql.SparkSession

// 1. Define the exact path to our file
val hdfsPath = "hdfs://namenode:8020/hello_hdfs.txt"

println("\n==========================================")
println("   STARTING SPARK-HDFS READ TEST")
println("==========================================")

try {
  // 2. Read the file
  val data = sc.textFile(hdfsPath)
  
  // 3. Force an action to verify accessibility
  val count = data.count()
  val content = data.first()

  // 4. Print Success Message
  println(s"✅ STATUS: SUCCESS")
  println(s"📂 File found at: $hdfsPath")
  println(s"📄 File Content:  $content")
  
} catch {
  case e: Exception => 
    println("❌ STATUS: FAILED")
    println("Could not read file. Error details:")
    e.printStackTrace()
}

println("==========================================\n")
System.exit(0)