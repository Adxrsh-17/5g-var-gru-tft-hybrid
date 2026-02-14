#!/bin/bash

# =====================================================
# 🚀 COMPLETE DECOUPLED 5G ANALYTICS PIPELINE 
# =====================================================
# Orchestrates all 3 phases of the Hybrid Architecture:
# Phase 1: Spark Streaming (PCAP → KPI Extraction → Real-time Aggregation)
# Phase 2: Python Training (Parquet → VAR+GRU+TFT → ONNX Export)  
# Phase 3: Scala ONNX Inference (Real-time Forecasting)

set -e
set -u

# =====================================================
# ⚙️ CONFIGURATION
# =====================================================
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
HDFS_NAMENODE="hdfs://namenode:8020"
JAR_PATH="target/5g-kpi-assembly-1.0.jar"

HDFS_PCAP_INPUT="/5g_kpi/pcap_files"       # Raw PCAP files
HDFS_KPI_PROCESSED="/5g_kpi/processed"     # Processed KPI Parquet files  
HDFS_STREAMING_INPUT="/5g_kpi/streaming_input" # Real-time streaming input
HDFS_PREDICTIONS="/5g_kpi/predictions"     # Real-time predictions output
HDFS_MODELS="/5g_kpi/models"               # Trained ONNX models

MODE="${1:-full}"  # Options: full, train-only, stream-only, batch-inference

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# =====================================================
# 🔨 BUILD & DEPLOY
# =====================================================
build_jar() {
    log "🔨 Building Scala JAR with ONNX Runtime..."
    
    if ! "C:\apache-maven\apache-maven-3.9.12\bin\mvn.cmd" clean package -DskipTests; then
        log "❌ Maven build failed!"
        exit 1
    fi
    
    if [[ ! -f "$JAR_PATH" ]]; then
        log "❌ JAR file not found at $JAR_PATH"
        exit 1
    fi
    
    JAR_SIZE=$(ls -lh "$JAR_PATH" | awk '{print $5}')
    log "✅ JAR built successfully: $JAR_PATH ($JAR_SIZE)"
}

# =====================================================
# 📊 PHASE 1: PCAP KPI EXTRACTION (Batch)
# =====================================================
extract_kpis() {
    log "📊 Phase 1: Extracting KPIs from PCAP files..."
    
    # Check if PCAP files exist
    if ! hdfs dfs -test -d "$HDFS_PCAP_INPUT" 2>/dev/null; then
        log "⚠️ No PCAP files found at $HDFS_PCAP_INPUT. Skipping extraction."
        return
    fi
    
    PCAP_COUNT=$(hdfs dfs -ls "$HDFS_PCAP_INPUT" | grep -c "\.pcap$" || echo "0")
    log "   Found $PCAP_COUNT PCAP files to process"
    
    if [[ "$PCAP_COUNT" -gt 0 ]]; then
        # Run PcapKpiExtractor
        $SPARK_HOME/bin/spark-submit \
            --class com.adarsh.kpi.PcapKpiExtractor \
            --master yarn \
            --deploy-mode cluster \
            --driver-memory 4g \
            --executor-memory 8g \
            --executor-cores 4 \
            --num-executors 8 \
            "$JAR_PATH" \
            "$HDFS_PCAP_INPUT" "$HDFS_KPI_PROCESSED" "eMBB"
            
        log "✅ KPI extraction complete. Output: $HDFS_KPI_PROCESSED"
    fi
}

# =====================================================
# 🧠 PHASE 2: PYTHON TRAINING PIPELINE
# =====================================================  
run_training() {
    log "🧠 Phase 2: Running Python Training Pipeline..."
    
    # Execute the enhanced training script
    if ! ./run_cluster_training.sh eMBB; then
        log "❌ Training pipeline failed!"
        exit 1
    fi
    
    # Verify model artifacts
    if hdfs dfs -test -f "$HDFS_MODELS/model_latest.onnx" && hdfs dfs -test -f "$HDFS_MODELS/scaler_latest.json"; then
        MODEL_SIZE=$(hdfs dfs -ls "$HDFS_MODELS/model_latest.onnx" | awk '{print $5}')
        log "✅ Training complete. Model size: ${MODEL_SIZE} bytes"
    else
        log "❌ Training artifacts not found in HDFS!"
        exit 1
    fi
}

# =====================================================
# ⚡ PHASE 3A: REAL-TIME STREAMING INFERENCE  
# =====================================================
start_streaming() {
    log "⚡ Phase 3A: Starting Real-time KPI Streaming..."
    
    # Ensure streaming input directory exists
    hdfs dfs -mkdir -p "$HDFS_STREAMING_INPUT" 2>/dev/null || true
    hdfs dfs -mkdir -p "$HDFS_PREDICTIONS" 2>/dev/null || true
    
    # Copy some processed KPIs to streaming input for testing
    if hdfs dfs -test -d "$HDFS_KPI_PROCESSED" 2>/dev/null; then
        log "   Copying sample data to streaming input for testing..."
        hdfs dfs -cp "$HDFS_KPI_PROCESSED"/*.parquet "$HDFS_STREAMING_INPUT/" 2>/dev/null || true
    fi
    
    # Start Spark Structured Streaming job
    log "   Launching Spark Structured Streaming with ONNX inference..."
    
    $SPARK_HOME/bin/spark-submit \
        --class com.adarsh.kpi.RealTimeKpiStreaming \
        --master yarn \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 6g \
        --executor-cores 2 \
        --num-executors 4 \
        --conf spark.sql.streaming.checkpointLocation="/tmp/spark-streaming-checkpoint" \
        "$JAR_PATH" &
        
    STREAMING_PID=$!
    log "✅ Streaming job started (PID: $STREAMING_PID)"
    log "   Monitor predictions at: $HDFS_PREDICTIONS"
    log "   Press Ctrl+C to stop streaming"
    
    # Wait for user interrupt
    trap "kill $STREAMING_PID 2>/dev/null || true; log 'Streaming stopped.'" INT
    wait $STREAMING_PID
}

# =====================================================
# 📈 PHASE 3B: BATCH INFERENCE (Testing)
# =====================================================
run_batch_inference() {
    log "📈 Phase 3B: Running Batch Inference for validation..."
    
    BATCH_OUTPUT="/tmp/batch_predictions_$(date +%s)"
    
    $SPARK_HOME/bin/spark-submit \
        --class com.adarsh.kpi.BatchKpiInference \
        --master local[4] \
        --driver-memory 4g \
        "$JAR_PATH" \
        "$HDFS_KPI_PROCESSED" "$BATCH_OUTPUT"
        
    log "✅ Batch inference complete. Results: $BATCH_OUTPUT"
    
    # Show sample predictions
    if [[ -d "$BATCH_OUTPUT" ]]; then
        PRED_COUNT=$(find "$BATCH_OUTPUT" -name "*.parquet" | wc -l)
        log "   Generated $PRED_COUNT prediction files"
        log "   View with: spark-shell --packages org.apache.spark:spark-sql_2.12:3.5.1"
        log "   > spark.read.parquet(\"$BATCH_OUTPUT\").show()"
    fi
}

# =====================================================
# 🎯 PIPELINE ORCHESTRATION
# =====================================================
case "$MODE" in
    "full")
        log "🚀 Running COMPLETE Decoupled 5G Analytics Pipeline"
        build_jar
        extract_kpis
        run_training  
        run_batch_inference
        start_streaming
        ;;
        
    "train-only")
        log "🧠 Training Pipeline Only"
        run_training
        ;;
        
    "stream-only") 
        log "⚡ Streaming Pipeline Only"
        build_jar
        start_streaming
        ;;
        
    "batch-inference")
        log "📈 Batch Inference Only" 
        build_jar
        run_batch_inference
        ;;
        
    "build")
        log "🔨 Build Only"
        build_jar
        ;;
        
    *)
        echo "Usage: $0 {full|train-only|stream-only|batch-inference|build}"
        echo ""
        echo "Modes:"
        echo "  full            - Complete pipeline (extract + train + inference + streaming)"
        echo "  train-only      - Python training pipeline only"  
        echo "  stream-only     - Real-time streaming inference only"
        echo "  batch-inference - Batch inference for testing"
        echo "  build          - Build JAR only"
        exit 1
        ;;
esac

log "🎉 Pipeline execution completed successfully!"