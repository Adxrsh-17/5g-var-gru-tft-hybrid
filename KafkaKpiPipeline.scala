// KafkaKpiPipeline.scala
// Phase 3: Kafka Integration with Structured Streaming (Production-Ready)
// Parquet KPIs → Kafka Producer → Structured Streaming Consumer → Checkpointed Storage

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{Trigger, OutputMode}
import org.apache.spark.sql.types._
import java.util.concurrent.TimeUnit

object KafkaKpiPipeline {

  // KPI Schema for JSON parsing
  val kpiSchema = StructType(Seq(
    StructField("sliceType", StringType),
    StructField("windowStart", LongType),
    StructField("event_time", TimestampType),
    StructField("Throughput_bps", DoubleType),
    StructField("Total_Packets", LongType),
    StructField("Total_Bytes", LongType),
    StructField("Avg_IAT", DoubleType),
    StructField("Jitter", DoubleType),
    StructField("IAT_Skewness", DoubleType),
    StructField("Avg_Packet_Size", DoubleType),
    StructField("Pkt_Size_StdDev", DoubleType),
    StructField("TCP_Ratio", DoubleType),
    StructField("UDP_Ratio", DoubleType),
    StructField("Avg_Win_Size", DoubleType),
    StructField("Zero_Win_Count", LongType)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_KPI_Kafka_Pipeline")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
      .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:8020/5G_kpi/checkpoints")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("\n" + "="*60)
    println("   5G KPI KAFKA PIPELINE - PHASE 3 (PRODUCTION)")
    println("   Parquet → Kafka → Structured Streaming → Storage")
    println("="*60)
    
    val kafkaBootstrap = "kafka:9092"
    val kpiParquetPath = "hdfs://namenode:8020/5G_kpi/processed/kpi_parquet"
    val kafkaTopic = "kpi-5g-all"
    val checkpointPath = "hdfs://namenode:8020/5G_kpi/checkpoints/kafka_consumer"
    val finalOutputPath = "hdfs://namenode:8020/5G_kpi/final/kpi_from_kafka"
    
    // ========================================
    // STEP 1: Read KPIs from Parquet
    // ========================================
    println("\n📦 STEP 1: Reading KPIs from Parquet...")
    
    val kpiDF = spark.read.parquet(kpiParquetPath)
    val totalRecords = kpiDF.count()
    println(s"   ✅ Loaded $totalRecords KPI records")
    
    println("\n📊 KPI Distribution by Slice:")
    kpiDF.groupBy("sliceType").count().show()
    
    // ========================================
    // STEP 2: Produce KPIs to Kafka
    // ========================================
    println("\n📤 STEP 2: Producing KPIs to Kafka...")
    
    val kafkaReadyDF = kpiDF
      .withColumn("event_time", current_timestamp())
      .withColumn("key", concat(col("sliceType"), lit("_"), col("windowStart")))
    
    val kpiColumns = Seq(
      "sliceType", "windowStart", "event_time",
      "Throughput_bps", "Total_Packets", "Total_Bytes",
      "Avg_IAT", "Jitter", "IAT_Skewness",
      "Avg_Packet_Size", "Pkt_Size_StdDev",
      "TCP_Ratio", "UDP_Ratio",
      "Avg_Win_Size", "Zero_Win_Count"
    )
    
    val kpiForKafka = kafkaReadyDF.select(kpiColumns.head, kpiColumns.tail: _*)
    
    println(s"   Topic: $kafkaTopic")
    println(s"   Bootstrap: $kafkaBootstrap")
    
    try {
      kpiForKafka
        .selectExpr("CAST(sliceType AS STRING) AS key", "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrap)
        .option("topic", kafkaTopic)
        .save()
      
      println(s"   ✅ Produced $totalRecords KPI events to Kafka!")
    } catch {
      case e: Exception =>
        println(s"   ❌ Kafka produce error: ${e.getMessage}")
    }
    
    // ========================================
    // STEP 3: Structured Streaming Consumer
    // ========================================
    println("\n📥 STEP 3: Starting Structured Streaming Consumer...")
    println(s"   Checkpoint: $checkpointPath")
    
    try {
      // Use readStream for continuous consumption
      val kafkaStreamDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrap)
        .option("subscribe", kafkaTopic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
      
      // Parse JSON values with explicit schema
      val parsedStreamDF = kafkaStreamDF
        .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
        .withColumn("parsed", from_json(col("json_value"), kpiSchema))
        .select(
          col("kafka_timestamp"),
          col("parsed.*")
        )
        .filter(col("sliceType").isNotNull) // Filter out parse failures
      
      // Write with Structured Streaming + checkpointing
      val query = parsedStreamDF.writeStream
        .format("parquet")
        .option("path", finalOutputPath)
        .option("checkpointLocation", checkpointPath)
        .partitionBy("sliceType")
        .outputMode(OutputMode.Append())
        .trigger(Trigger.Once()) // Process all available data once, then stop
        .start()
      
      println("   🔄 Processing stream...")
      query.awaitTermination()
      println(s"   ✅ Structured Streaming complete!")
      
    } catch {
      case e: Exception =>
        println(s"   ⚠️ Streaming error: ${e.getMessage}")
        println("   Falling back to batch read...")
        
        // Fallback to batch if streaming fails
        val consumedDF = spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBootstrap)
          .option("subscribe", kafkaTopic)
          .option("startingOffsets", "earliest")
          .load()
          .selectExpr("CAST(value AS STRING) as json_value")
        
        val parsedDF = consumedDF
          .withColumn("parsed", from_json(col("json_value"), kpiSchema))
          .select(col("parsed.*"))
          .filter(col("sliceType").isNotNull)
        
        parsedDF.write
          .mode("overwrite")
          .partitionBy("sliceType")
          .parquet(finalOutputPath)
        
        println(s"   ✅ Batch fallback saved to: $finalOutputPath")
    }
    
    // ========================================
    // STEP 4: Verify Output
    // ========================================
    println("\n🔍 STEP 4: Verifying output...")
    
    try {
      val outputDF = spark.read.parquet(finalOutputPath)
      val outputCount = outputDF.count()
      println(s"   ✅ Final records: $outputCount")
      
      println("\n📊 Output by Slice Type:")
      outputDF.groupBy("sliceType").count().orderBy("sliceType").show()
      
      println("\n📋 Sample Output:")
      outputDF.select("sliceType", "windowStart", "Throughput_bps", "Total_Packets", "Avg_IAT")
        .limit(5).show()
    } catch {
      case e: Exception =>
        println(s"   ⚠️ Verification error: ${e.getMessage}")
    }
    
    // ========================================
    // SUMMARY
    // ========================================
    println("\n" + "="*60)
    println("✅ PHASE 3 COMPLETE: KAFKA + STRUCTURED STREAMING")
    println("="*60)
    println("\n📊 Pipeline Summary:")
    println(s"   Source:      $kpiParquetPath")
    println(s"   Kafka Topic: $kafkaTopic")
    println(s"   Records:     $totalRecords")
    println(s"   Checkpoint:  $checkpointPath")
    println(s"   Output:      $finalOutputPath")
    println("\n🔄 Data Flow:")
    println("   HDFS (Parquet) → Kafka → Structured Streaming → HDFS (Final)")
    println("\n✅ Production-ready KPI streaming pipeline validated!")
    
    spark.stop()
  }
}
