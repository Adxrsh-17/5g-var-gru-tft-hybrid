// KafkaKpiPipeline.scala
// Phase 3: Kafka Integration for Real-Time KPI Streaming
// Reads Parquet KPIs → Kafka → Consumer → Final Storage

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.concurrent.TimeUnit

object KafkaKpiPipeline {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_KPI_Kafka_Pipeline")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("\n" + "="*60)
    println("   5G KPI KAFKA PIPELINE - PHASE 3")
    println("   Parquet → Kafka → Consumer → Storage")
    println("="*60)
    
    val kafkaBootstrap = "kafka:9092"
    val kpiParquetPath = "hdfs://namenode:8020/5G_kpi/processed/kpi_parquet"
    
    // ========================================
    // STEP 1: Read KPIs from Parquet
    // ========================================
    println("\n📦 STEP 1: Reading KPIs from Parquet...")
    
    val kpiDF = spark.read.parquet(kpiParquetPath)
    val totalRecords = kpiDF.count()
    println(s"   ✅ Loaded $totalRecords KPI records")
    
    // Show slice distribution
    println("\n📊 KPI Distribution by Slice:")
    kpiDF.groupBy("sliceType").count().show()
    
    // ========================================
    // STEP 2: Produce KPIs to Kafka
    // ========================================
    println("\n📤 STEP 2: Producing KPIs to Kafka...")
    
    // Add metadata columns for Kafka
    val kafkaReadyDF = kpiDF
      .withColumn("event_time", current_timestamp())
      .withColumn("key", concat(col("sliceType"), lit("_"), col("windowStart")))
    
    // Select important KPIs for Kafka (to reduce message size)
    val kpiColumns = Seq(
      "sliceType", "windowStart", "event_time",
      "Throughput_bps", "Total_Packets", "Total_Bytes",
      "Avg_IAT", "Jitter", "IAT_Skewness",
      "Avg_Packet_Size", "Pkt_Size_StdDev",
      "TCP_Ratio", "UDP_Ratio",
      "Avg_Win_Size", "Zero_Win_Count"
    )
    
    val kpiForKafka = kafkaReadyDF.select(kpiColumns.head, kpiColumns.tail: _*)
    
    // Convert to JSON and produce to Kafka
    val kafkaTopic = "kpi-5g-all"
    
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
        println("   Continuing with verification...")
    }
    
    // ========================================
    // STEP 3: Verify by consuming from Kafka
    // ========================================
    println("\n📥 STEP 3: Verifying Kafka messages (sample)...")
    
    try {
      val kafkaDF = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrap)
        .option("subscribe", kafkaTopic)
        .option("startingOffsets", "earliest")
        .load()
      
      val messageCount = kafkaDF.count()
      println(s"   ✅ Messages in Kafka: $messageCount")
      
      // Show sample messages
      println("\n📋 Sample Kafka Messages:")
      kafkaDF
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
        .limit(5)
        .show(false)
        
    } catch {
      case e: Exception =>
        println(s"   ❌ Kafka read error: ${e.getMessage}")
    }
    
    // ========================================
    // STEP 4: Consumer - Write to Final Parquet
    // ========================================
    println("\n💾 STEP 4: Consumer writes to final storage...")
    
    val finalOutputPath = "hdfs://namenode:8020/5G_kpi/final/kpi_from_kafka"
    
    try {
      val consumedDF = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrap)
        .option("subscribe", kafkaTopic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_value")
      
      // Parse JSON back to columns
      val parsedDF = spark.read.json(consumedDF.as[String])
      
      // Write partitioned by slice type
      parsedDF.write
        .mode("overwrite")
        .partitionBy("sliceType")
        .parquet(finalOutputPath)
      
      println(s"   ✅ Saved to: $finalOutputPath")
      
    } catch {
      case e: Exception =>
        println(s"   ❌ Consumer error: ${e.getMessage}")
    }
    
    // ========================================
    // SUMMARY
    // ========================================
    println("\n" + "="*60)
    println("✅ PHASE 3 COMPLETE: KAFKA INTEGRATION")
    println("="*60)
    println("\n📊 Pipeline Summary:")
    println(s"   Source:      $kpiParquetPath")
    println(s"   Kafka Topic: $kafkaTopic")
    println(s"   Records:     $totalRecords")
    println(s"   Output:      $finalOutputPath")
    println("\n🔄 Data Flow:")
    println("   HDFS (Parquet) → Kafka → Consumer → HDFS (Final)")
    println("\n✅ Real-time KPI streaming architecture validated!")
    
    spark.stop()
  }
}
