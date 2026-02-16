package com.adarsh.kpi

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.streaming.{StreamingQuery, GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import scala.collection.mutable

/**
 * REAL-TIME 5G KPI STREAMING ANALYTICS WITH ONNX INFERENCE
 * 
 * This component implements Phase 1 & 3 of the Decoupled Hybrid Architecture:
 * - Reads PCAP data from HDFS via Spark Structured Streaming
 * - Aggregates 36 KPIs using mapGroupsWithState (60s sliding window)
 * - Performs real-time inference using ONNX Runtime for millisecond latency
 * - Outputs predictions to HDFS for downstream consumption
 */
object RealTimeKpiStreaming {

  // Configuration
  case class Config(
    inputPath: String = "hdfs://namenode:8020/5g_kpi/streaming_input",
    outputPath: String = "hdfs://namenode:8020/5g_kpi/predictions",
    modelPath: String = "hdfs://namenode:8020/5g_kpi/models/model_latest.onnx",
    scalerPath: String = "hdfs://namenode:8020/5g_kpi/models/scaler_latest.json",
    windowDuration: String = "60 seconds",
    slideDuration: String = "10 seconds",
    triggerInterval: String = "5 seconds"
  )

  // KPI Window State for mapGroupsWithState
  case class KpiWindowState(
    cellId: Int,
    sliceType: String,
    window: mutable.Queue[KpiMetrics],
    lastUpdated: Long
  )

  // Real-time KPI metrics (36 features aggregated from PCAP)
  case class KpiMetrics(
    timestamp: Timestamp,
    cellId: Int,
    sliceType: String,
    throughput_mbps: Double,
    jitter_ms: Double,
    packet_loss_rate: Double,
    cpu_utilization: Double,
    memory_utilization: Double,
    // Additional 29 KPIs from your PcapKpiExtractor
    rsrp_dbm: Double,
    rsrq_db: Double,
    sinr_db: Double,
    bler_percentage: Double,
    handover_count: Int,
    paging_success_rate: Double,
    rach_success_rate: Double,
    // ... extend with all 36 KPIs as needed
    features: Array[Double] // All 36 features as array for ONNX
  )

  // Prediction output
  case class KpiPrediction(
    timestamp: Timestamp,
    cellId: Int,
    sliceType: String,
    predicted_throughput: Float,
    predicted_jitter: Float,
    predicted_packet_loss: Float,
    predicted_cpu: Float,
    predicted_memory: Float,
    predicted_rsrp: Float,
    predicted_rsrq: Float,
    confidence_score: Double,
    model_version: String
  )

  def main(args: Array[String]): Unit = {
    val config = Config() // Use default or parse from args

    val spark = SparkSession.builder()
      .appName("5G-RealTime-KPI-Analytics")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    import spark.implicits._
    
    // Define explicit encoders for case classes
    implicit val kpiMetricsEncoder = org.apache.spark.sql.Encoders.product[KpiMetrics]
    implicit val kpiPredictionEncoder = org.apache.spark.sql.Encoders.product[KpiPrediction]

    // Initialize ONNX Runtime (one-time setup)
    try {
      OnnxInferenceWrapper.init(config.modelPath, config.scalerPath)
      println(s"🚀 ONNX Model loaded: ${config.modelPath}")
    } catch {
      case e: Exception =>
        println(s"❌ Failed to load ONNX model: ${e.getMessage}")
        spark.stop()
        return
    }

    // Phase 1: Read streaming PCAP data from HDFS
    val kpiStream = spark
      .readStream
      .format("parquet") // Assuming PcapKpiExtractor outputs Parquet to streaming folder
      .option("path", config.inputPath)
      .option("maxFilesPerTrigger", "10") // Process 10 files per batch
      .load()
      .as[KpiMetrics]

    println(s"📡 Started streaming from: ${config.inputPath}")

    // Phase 3: Apply sliding window aggregation + ONNX inference
    val predictions = kpiStream
      .groupByKey(kpi => s"${kpi.cellId}_${kpi.sliceType}") // Group by Cell+Slice
      .flatMapGroupsWithState(org.apache.spark.sql.streaming.OutputMode.Update, GroupStateTimeout.ProcessingTimeTimeout)(updateKpiWindow)

    // Output predictions to HDFS for downstream consumption
    val query: StreamingQuery = predictions
      .writeStream
      .outputMode("update")
      .format("parquet")
      .option("path", config.outputPath)
      .option("checkpointLocation", "/tmp/spark-checkpoint/kpi-predictions")
      .trigger(Trigger.ProcessingTime(config.triggerInterval))
      .start()

    println(s"🎯 Predictions streaming to: ${config.outputPath}")
    println("🔥 Real-time KPI forecasting active. Press Ctrl+C to stop.")

    // Keep streaming alive
    query.awaitTermination()

    // Cleanup
    OnnxInferenceWrapper.close()
    spark.stop()
  }

  /**
   * Stateful streaming function using mapGroupsWithState
   * Maintains a 60-second sliding window of KPI metrics per Cell+Slice
   * Triggers ONNX inference when window is full (60 samples)
   */
  def updateKpiWindow(
    key: String,
    values: Iterator[KpiMetrics],
    state: GroupState[KpiWindowState]
  ): Iterator[KpiPrediction] = {

    val currentTime = System.currentTimeMillis()
    
    // Initialize or retrieve existing window state
    val windowState = if (state.exists) {
      state.get
    } else {
      // Parse key to extract cellId and sliceType
      val Array(cellIdStr, sliceType) = key.split("_")
      KpiWindowState(
        cellId = cellIdStr.toInt,
        sliceType = sliceType,
        window = mutable.Queue[KpiMetrics](),
        lastUpdated = currentTime
      )
    }

    // Add new KPI samples to the sliding window
    values.foreach { kpi =>
      windowState.window.enqueue(kpi)
      
      // Maintain 60-second window (assuming 1-second samples)
      while (windowState.window.size > 60) {
        windowState.window.dequeue()
      }
    }

    // Update state
    val updatedState = windowState.copy(lastUpdated = currentTime)
    state.update(updatedState)

    // Set timeout for state cleanup (remove inactive keys)
    state.setTimeoutDuration("300 seconds") // 5 minutes timeout

    // Perform inference if we have sufficient data (at least 60 samples)
    if (updatedState.window.size >= 60) {
      try {
        // Prepare ONNX input: Extract features from the last 60 samples
        val windowData = updatedState.window.takeRight(60).map(_.features).toArray
        
        // Run ONNX inference (returns predicted values for next timestep)
        val prediction = OnnxInferenceWrapper.predict(windowData)
        
        // Create prediction result
        val result = KpiPrediction(
          timestamp = new Timestamp(currentTime),
          cellId = updatedState.cellId,
          sliceType = updatedState.sliceType,
          predicted_throughput = prediction(0),
          predicted_jitter = prediction(1),
          predicted_packet_loss = prediction(2),
          predicted_cpu = prediction(3),
          predicted_memory = prediction(4),
          predicted_rsrp = if (prediction.length > 5) prediction(5) else 0.0f,
          predicted_rsrq = if (prediction.length > 6) prediction(6) else 0.0f,
          confidence_score = calculateConfidence(windowData, prediction),
          model_version = "latest"
        )

        Iterator(result)

      } catch {
        case e: Exception =>
          println(s"⚠️ Inference failed for key $key: ${e.getMessage}")
          Iterator.empty
      }
    } else {
      // Not enough data for prediction yet
      Iterator.empty
    }
  }

  /**
   * Calculate confidence score based on input window variance
   * Higher variance = lower confidence
   */
  private def calculateConfidence(windowData: Array[Array[Double]], prediction: Array[Float]): Double = {
    try {
      // Calculate variance across the window for throughput (feature 0)
      val throughputValues = windowData.map(_(0))
      val mean = throughputValues.sum / throughputValues.length
      val variance = throughputValues.map(v => math.pow(v - mean, 2)).sum / throughputValues.length
      
      // Normalize to confidence score (0.0 to 1.0)
      val confidence = math.max(0.1, 1.0 - (variance / 1000.0)) // Adjust scale as needed
      math.min(1.0, confidence)
    } catch {
      case _: Exception => 0.5 // Default confidence
    }
  }
}

/**
 * BATCH INFERENCE MODE (Alternative to Streaming)
 * For testing and validation purposes
 */
object BatchKpiInference {
  
  def runBatchInference(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    import spark.implicits._

    // Initialize ONNX
    OnnxInferenceWrapper.init("hdfs://namenode:8020/5g_kpi/models/model_latest.onnx")

    // Read batch KPI data
    val kpiData = spark.read.parquet(inputPath).as[RealTimeKpiStreaming.KpiMetrics]

    // Group by Cell+Slice and create windows  
    val predictions = kpiData
      .groupByKey(kpi => s"${kpi.cellId}_${kpi.sliceType}")
      .flatMapGroups { case (key, kpis) =>
        val kpiList = kpis.toList.sortBy(_.timestamp.getTime)
        
        // Process in 60-sample sliding windows
        kpiList.sliding(60, 10).flatMap { window =>
          if (window.size == 60) {
            try {
              val windowData = window.map(_.features).toArray
              val prediction = OnnxInferenceWrapper.predict(windowData)
              
              Some(RealTimeKpiStreaming.KpiPrediction(
                timestamp = window.last.timestamp,
                cellId = window.head.cellId,
                sliceType = window.head.sliceType,
                predicted_throughput = prediction(0),
                predicted_jitter = prediction(1),
                predicted_packet_loss = prediction(2),
                predicted_cpu = prediction(3),
                predicted_memory = prediction(4),
                predicted_rsrp = if (prediction.length > 5) prediction(5) else 0.0f,
                predicted_rsrq = if (prediction.length > 6) prediction(6) else 0.0f,
                confidence_score = 0.8, // Simplified for batch
                model_version = "batch_test"
              ))
            } catch {
              case e: Exception =>
                println(s"Batch inference failed: ${e.getMessage}")
                None
            }
          } else None
        }
      }

    // Save batch predictions
    predictions.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"✅ Batch inference complete. Results saved to: $outputPath")
  }
}