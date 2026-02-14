#!/bin/bash

# =====================================================
# 🎯 SLICE-WISE 5G MODEL TRAINING PIPELINE  
# =====================================================
# Automates training of separate models for each network slice:
# - eMBB (Enhanced Mobile Broadband)
# - URLLC (Ultra-Reliable Low Latency Communications)  
# - mMTC (Massive Machine Type Communications)
#
# Pipeline: HDFS Download → Train → Upload → Cleanup

set -e  # Exit on any error
set -u  # Exit on undefined variables

# =====================================================
# ⚙️ CONFIGURATION
# =====================================================
HDFS_ROOT="/5g_kpi"
SLICES=("URLLC" "mMTC")
ARTIFACTS_DIR="./artifacts"
EPOCHS=50  # Reduced for faster iteration

# Logging with timestamps
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Error handling
error_exit() {
    log "❌ ERROR: $1"
    exit 1
}

# No local cleanup needed for direct HDFS loading

# =====================================================
# 🔍 PRE-FLIGHT CHECKS
# =====================================================
log "🚀 Starting Slice-Wise Training Pipeline"
log "📋 Target Slices: ${SLICES[*]}"

# Check Python dependencies
if ! /opt/python3.9.12/bin/python3 -c "import tensorflow, tf2onnx, sklearn, pandas, numpy, statsmodels" 2>/dev/null; then
    error_exit "Required Python packages missing. Install: pip3 install tensorflow tf2onnx scikit-learn pandas numpy statsmodels"
fi

# Check HDFS connectivity
if ! hdfs dfs -test -d "$HDFS_ROOT/processed" 2>/dev/null; then
    error_exit "HDFS directory $HDFS_ROOT/processed not accessible"
fi

# Create local directories
mkdir -p "$LOCAL_STAGING" "$ARTIFACTS_DIR"

log "✅ Pre-flight checks passed"

# =====================================================
# 📊 SLICE TRAINING LOOP
# =====================================================
SUCCESSFUL_SLICES=()
FAILED_SLICES=()

for slice in "${SLICES[@]}"; do
    log ""
    log "=" * 60
    log "🎯 Processing Slice: $slice"
    log "=" * 60
    
    # SLICE_DIR="$LOCAL_STAGING/$slice" # Unused
    HDFS_SLICE_PATH="$HDFS_ROOT/processed/sliceType=$slice"
    HDFS_MODEL_PATH="$HDFS_ROOT/models/$slice"
    
    # Step 1: Check if slice data exists in HDFS
    log "🔍 Checking HDFS data for slice $slice..."
    
    if ! hdfs dfs -test -d "$HDFS_SLICE_PATH" 2>/dev/null; then
        log "⚠️ No data found for slice $slice at $HDFS_SLICE_PATH. Skipping..."
        FAILED_SLICES+=("$slice (no data)")
        continue
    fi
    
    FILE_COUNT=$(hdfs dfs -ls "$HDFS_SLICE_PATH" 2>/dev/null | grep -c "\.parquet$" || echo "0")
    if [[ "$FILE_COUNT" -eq 0 ]]; then
        log "⚠️ No Parquet files found for slice $slice. Skipping..."
        FAILED_SLICES+=("$slice (no parquet files)")
        continue
    fi
    
    log "   Found $FILE_COUNT Parquet files for $slice"
    
    # Step 3: Direct HDFS Loading (No Local Download)
    log "🔍 Verifying HDFS data for $slice..."
    
    # Check if data exists in HDFS
    if ! hdfs dfs -test -e "$HDFS_SLICE_PATH" 2>/dev/null; then
         log "❌ HDFS Path not found: $HDFS_SLICE_PATH"
         FAILED_SLICES+=("$slice (no data)")
         continue
    fi
    
    log "   ✅ Data available at $HDFS_SLICE_PATH"
    
    # Step 3.5: Download existing checkpoint if available
    HDFS_CHECKPOINT_DIR="$HDFS_ROOT/checkpoints/$slice"
    CHECKPOINT_FILE="checkpoint_${slice}.weights.h5"
    
    log "🔄 Checking for existing checkpoint in HDFS..."
    if hdfs dfs -test -f "$HDFS_CHECKPOINT_DIR/$CHECKPOINT_FILE" 2>/dev/null; then
        log "   Found checkpoint! Downloading..."
        hdfs dfs -get "$HDFS_CHECKPOINT_DIR/$CHECKPOINT_FILE" "$ARTIFACTS_DIR/$CHECKPOINT_FILE"
    else
        log "   No checkpoint found. Starting fresh."
    fi

    # Step 4: Train the model for this slice
    log "🧠 Training model for slice $slice..."
    
    TRAINING_LOG="$ARTIFACTS_DIR/training_${slice}.log"
    
    log "🔎 DEBUG: Using Data Dir: $HDFS_SLICE_PATH"
    
    # Use direct redirection instead of tee to avoid binary corruption
    # Pass HDFS path directly to --data_dir
    if /opt/python3.9.12/bin/python3 train.py \
        --data_dir "$HDFS_SLICE_PATH" \
        --slice_name "$slice" \
        --output_dir "$ARTIFACTS_DIR" \
        --epochs "$EPOCHS" \
        --window 60 \
        > "$TRAINING_LOG" 2>&1; then
        
        log "✅ Training completed for $slice"
        cat "$TRAINING_LOG" | tail -n 5  # Show last few lines
        
        # Verify artifacts were generated
        if [[ -f "$ARTIFACTS_DIR/model.onnx" && -f "$ARTIFACTS_DIR/scaler_params.json" ]]; then
            MODEL_SIZE=$(ls -lh "$ARTIFACTS_DIR/model.onnx" | awk '{print $5}')
            log "   Generated artifacts: model.onnx ($MODEL_SIZE), scaler_params.json"
        else
            log "❌ Training artifacts not found for $slice"
            # Show error log
            cat "$TRAINING_LOG" | tail -n 20
            FAILED_SLICES+=("$slice (missing artifacts)")
            continue
        fi
    else
        log "❌ Training failed for slice $slice. Check $TRAINING_LOG"
        cat "$TRAINING_LOG" | tail -n 20 # Show error log
        FAILED_SLICES+=("$slice (training failed)")
        
        # Even if training failed, try to save the checkpoint if it exists
        if [[ -f "$ARTIFACTS_DIR/$CHECKPOINT_FILE" ]]; then
             log "⚠️ Attempting to save partial checkpoint..."
             hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_DIR" 2>/dev/null || true
             hdfs dfs -put -f "$ARTIFACTS_DIR/$CHECKPOINT_FILE" "$HDFS_CHECKPOINT_DIR/$CHECKPOINT_FILE"
             log "   💾 Partial checkpoint saved to HDFS."
        fi
        
        continue
    fi
    
    # Step 4.5: Upload Checkpoint to HDFS for future runs
    if [[ -f "$ARTIFACTS_DIR/$CHECKPOINT_FILE" ]]; then
        log "💾 Syncing checkpoint to HDFS..."
        hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_DIR" 2>/dev/null || true
        hdfs dfs -put -f "$ARTIFACTS_DIR/$CHECKPOINT_FILE" "$HDFS_CHECKPOINT_DIR/$CHECKPOINT_FILE"
        log "   ✅ Checkpoint synced: $HDFS_CHECKPOINT_DIR/$CHECKPOINT_FILE"
    fi
    
    # Step 5: Upload models to HDFS
    log "📤 Uploading model artifacts for $slice to HDFS..."
    
    # Create HDFS model directory
    hdfs dfs -mkdir -p "$HDFS_MODEL_PATH" 2>/dev/null || true
    
    # Upload with versioning
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    
    # Upload ONNX model
    if hdfs dfs -put -f "$ARTIFACTS_DIR/model.onnx" "$HDFS_MODEL_PATH/model_${TIMESTAMP}.onnx" 2>/dev/null; then
        # Create latest symlink
        hdfs dfs -rm "$HDFS_MODEL_PATH/model_latest.onnx" 2>/dev/null || true
        hdfs dfs -cp "$HDFS_MODEL_PATH/model_${TIMESTAMP}.onnx" "$HDFS_MODEL_PATH/model_latest.onnx"
        log "   ✅ ONNX model uploaded: $HDFS_MODEL_PATH/model_latest.onnx"
    else
        log "❌ Failed to upload ONNX model for $slice"
        FAILED_SLICES+=("$slice (onnx upload failed)")
        continue
    fi
    
    # Upload scaler parameters
    if hdfs dfs -put -f "$ARTIFACTS_DIR/scaler_params.json" "$HDFS_MODEL_PATH/scaler_${TIMESTAMP}.json" 2>/dev/null; then
        # Create latest symlink
        hdfs dfs -rm "$HDFS_MODEL_PATH/scaler_latest.json" 2>/dev/null || true
        hdfs dfs -cp "$HDFS_MODEL_PATH/scaler_${TIMESTAMP}.json" "$HDFS_MODEL_PATH/scaler_latest.json"
        log "   ✅ Scaler params uploaded: $HDFS_MODEL_PATH/scaler_latest.json"
    else
        log "❌ Failed to upload scaler params for $slice"
        FAILED_SLICES+=("$slice (scaler upload failed)")
        continue
    fi
    
    # Cleanup local artifacts for next iteration
    rm -f "$ARTIFACTS_DIR/model.onnx" "$ARTIFACTS_DIR/scaler_params.json"
    
    # Step 6: Cleanup local data (Not needed for direct HDFS)
    # cleanup_slice "$slice"
    
    SUCCESSFUL_SLICES+=("$slice")
    log "🎉 Successfully processed slice: $slice"
done

# =====================================================
# 📊 FINAL REPORT
# =====================================================
log ""
log "=" * 60
log "🏁 SLICE-WISE TRAINING PIPELINE COMPLETED"
log "=" * 60

log "📈 SUCCESSFUL SLICES (${#SUCCESSFUL_SLICES[@]}):"
for slice in "${SUCCESSFUL_SLICES[@]}"; do
    log "   ✅ $slice"
done

if [[ ${#FAILED_SLICES[@]} -gt 0 ]]; then
    log "❌ FAILED SLICES (${#FAILED_SLICES[@]}):"
    for slice in "${FAILED_SLICES[@]}"; do
        log "   ❌ $slice"
    done
fi

# Generate HDFS model inventory
log ""
log "📦 HDFS MODEL INVENTORY:"
if hdfs dfs -test -d "$HDFS_ROOT/models" 2>/dev/null; then
    hdfs dfs -ls -R "$HDFS_ROOT/models" 2>/dev/null | grep -E "(model_latest\.onnx|scaler_latest\.json)" | while read -r line; do
        log "   📁 $(echo "$line" | awk '{print $8}')"
    done
else
    log "   ⚠️ No models directory found in HDFS"
fi

# Exit status
if [[ ${#SUCCESSFUL_SLICES[@]} -gt 0 ]]; then
    log ""
    log "🎯 NEXT STEPS:"
    log "   1. Update Scala inference code to load slice-specific models"
    log "   2. Test inference with: OnnxInferenceWrapper.init(\"/5g_kpi/models/{SLICE}/model_latest.onnx\")"
    log "   3. Deploy to production Spark Streaming pipeline"
    
    exit 0
else
    log ""
    log "💥 CRITICAL: No slices were successfully processed!"
    exit 1
fi