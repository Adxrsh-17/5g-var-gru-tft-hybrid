package com.adarsh.kpi

import ai.onnxruntime._
import java.nio.FloatBuffer
import java.util.Collections
import scala.collection.JavaConverters._
import java.io.File
import scala.util.{Try, Success, Failure}
import scala.io.Source

/**
 * WRAPPER FOR ONNX RUNTIME IN SPARK
 * Handles model loading, tensor conversion, and inference.
 * Designed as a Singleton to run efficiently on Spark Executors.
 */
object OnnxInferenceWrapper {

  // --- CONFIGURATION ---
  // Ensure this matches train.py CONFIG['window']
  private val SEQUENCE_LENGTH = 60 
  // Ensure this matches train.py FEATURE_MAP keys (Throughput, Jitter, etc.)
  private val FEATURE_COUNT = 7    
  
  @transient private var env: OrtEnvironment = _
  @transient private var session: OrtSession = _
  
  // RobustScaler parameters (loaded dynamically from scaler_params.json)
  @transient private var scalerCenter: Array[Float] = _
  @transient private var scalerScale: Array[Float] = _
  @transient private var isScalerLoaded: Boolean = false

  /**
   * Loads scaler parameters from JSON file exported by train.py
   */
  private def loadScalerParams(scalerPath: String): Unit = {
    if (!isScalerLoaded) {
      try {
        val source = Source.fromFile(scalerPath)
        val jsonContent = source.mkString
        source.close()
        
        // Simple JSON parsing for center and scale arrays
        val centerStart = jsonContent.indexOf("\"center\": [") + 11
        val centerEnd = jsonContent.indexOf("]", centerStart)
        val centerStr = jsonContent.substring(centerStart, centerEnd)
        scalerCenter = centerStr.split(",").map(_.trim.toFloat)
        
        val scaleStart = jsonContent.indexOf("\"scale\": [") + 10
        val scaleEnd = jsonContent.indexOf("]", scaleStart)
        val scaleStr = jsonContent.substring(scaleStart, scaleEnd)
        scalerScale = scaleStr.split(",").map(_.trim.toFloat)
        
        if (scalerCenter.length != FEATURE_COUNT || scalerScale.length != FEATURE_COUNT) {
          throw new RuntimeException(s"Scaler params mismatch: expected $FEATURE_COUNT features")
        }
        
        isScalerLoaded = true
        println(s"[ONNX] Loaded scaler parameters: center=${scalerCenter.mkString(",")}")
        
      } catch {
        case e: Exception =>
          System.err.println(s"[ERROR] Failed to load scaler params from $scalerPath: ${e.getMessage}")
          // Fallback to identity scaling
          scalerCenter = Array.fill(FEATURE_COUNT)(0.0f)
          scalerScale = Array.fill(FEATURE_COUNT)(1.0f)
          isScalerLoaded = true
          println("[WARN] Using identity scaling as fallback")
      }
    }
  }

  /**
   * Initializes the ONNX session and loads scaler parameters.
   * This is called once per Executor JVM.
   * @param modelPath Path to the .onnx file
   * @param scalerPath Path to the scaler_params.json file
   */
  def init(modelPath: String, scalerPath: String = "scaler_params.json"): Unit = {
    if (session == null) {
      synchronized {
        if (session == null) {
          println(s"[ONNX] Initializing Inference Engine with model: $modelPath")
          
          // Load scaler parameters first
          loadScalerParams(scalerPath)
          
          try {
            if (env == null) env = OrtEnvironment.getEnvironment("5G_Traffic_Forecaster")
            val opts = new OrtSession.SessionOptions()
            
            // OPTIONAL: Enable GPU if available on worker nodes
            // try { opts.addCUDA(0) } catch { case e: Exception => println("[WARN] CUDA not found, falling back to CPU") }
            
            session = env.createSession(modelPath, opts)
            println("[ONNX] Session created successfully.")
          } catch {
            case e: Exception => 
              System.err.println(s"[ERROR] Failed to load ONNX model: ${e.getMessage}")
              throw e
          }
        }
      }
    }
  }

  /**
   * Performs Inference on a window of data.
   * * @param windowData A 2D array of shape [60, 7] (TimeSteps x Features)
   * @return Predicted residual array for the next timestep (size 7)
   */
  def predict(windowData: Array[Array[Double]]): Array[Float] = {
    if (session == null) throw new IllegalStateException("Model not initialized! Call init() first.")
    if (!isScalerLoaded) throw new IllegalStateException("Scaler not loaded! Ensure scaler_params.json is available.")
    
    // 1. Validate Input Shape
    if (windowData.length != SEQUENCE_LENGTH) {
      throw new IllegalArgumentException(s"Expected window length $SEQUENCE_LENGTH, got ${windowData.length}")
    }

    // 2. Preprocess: Apply RobustScaler and Flatten
    val flatInput = new Array[Float](SEQUENCE_LENGTH * FEATURE_COUNT)
    var idx = 0
    
    for (t <- 0 until SEQUENCE_LENGTH) {
      val row = windowData(t)
      if (row.length != FEATURE_COUNT) 
        throw new IllegalArgumentException(s"Expected $FEATURE_COUNT features, got ${row.length} at step $t")
      
      for (f <- 0 until FEATURE_COUNT) {
        // Apply RobustScaler normalization: (x - center) / scale
        val scaledVal = ((row(f) - scalerCenter(f)) / scalerScale(f)).toFloat
        flatInput(idx) = scaledVal
        idx += 1
      }
    }

    // 3. Create ONNX Tensor with proper cleanup
    val shape = Array(1L, SEQUENCE_LENGTH.toLong, FEATURE_COUNT.toLong)
    var inputTensor: OnnxTensor = null
    var results: OrtSession.Result = null
    
    try {
      inputTensor = OnnxTensor.createTensor(env, FloatBuffer.wrap(flatInput), shape)
      
      // 4. Run Inference
      val inputs = Collections.singletonMap("input", inputTensor)
      results = session.run(inputs)
      
      // 5. Extract Result
      val outputTensor = results.get(0).getValue.asInstanceOf[Array[Array[Float]]]
      val rawPrediction = outputTensor(0)
      
      // 6. Post-process (Inverse Scaling) to get real units
      val finalPrediction = new Array[Float](FEATURE_COUNT)
      for (i <- 0 until FEATURE_COUNT) {
        finalPrediction(i) = rawPrediction(i) * scalerScale(i) + scalerCenter(i)
      }
      
      finalPrediction
      
    } catch {
      case e: Exception =>
        System.err.println(s"[ERROR] Inference failed: ${e.getMessage}")
        throw e
    } finally {
      // Critical: Close tensors to prevent memory leaks in long-running Spark jobs
      if (inputTensor != null) inputTensor.close()
      if (results != null) results.close()
    }
  }
  
  /**
   * Helper to close session on shutdown (optional)
   */
  def close(): Unit = {
    if (session != null) session.close()
    if (env != null) env.close()
  }
}