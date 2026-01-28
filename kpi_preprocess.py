from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer, StringIndexer
import sys

# ---------------------------------------------------------
# 1. INITIALIZE SPARK SESSION
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("KPI_Preprocessing_Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(">>> [INIT] Spark Session Created Successfully")

# ---------------------------------------------------------
# 2. DEFINE HDFS PATHS
# ---------------------------------------------------------
# UPDATED: Using port 8020 for both Input and Output
input_path = "hdfs://namenode:8020/data/raw/40_kpi_output.csv"
output_path = "hdfs://namenode:8020/data/processed/kpi_features"

# ---------------------------------------------------------
# 3. LOAD DATA
# ---------------------------------------------------------
print(f">>> [LOAD] Reading data from: {input_path}")
try:
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.printSchema()
except Exception as e:
    print(f">>> [ERROR] Could not read file. Error: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# 4. DYNAMIC COLUMN HANDLING
# ---------------------------------------------------------
print(">>> [PREP] Analyzing Column Types...")

# --- CRITICAL UPDATE HERE ---
# Added 'Header_Overhead_Ratio' to exclude it from the feature vector.
# This ensures the dataset is informationally independent (removes redundancy with UDP_Ratio).
excluded_cols = ['id', 'timestamp', 'date', '_c0', 'Serial_No', 'Header_Overhead_Ratio'] 

# Separate remaining columns into Numerical vs Categorical (String)
numeric_cols = []
string_cols = []

for field in df.schema.fields:
    if field.name in excluded_cols:
        continue
    
    # Check data type
    dtype = str(field.dataType)
    if "StringType" in dtype:
        string_cols.append(field.name)
    else:
        # Integers, Doubles, Floats, Longs
        numeric_cols.append(field.name)

print(f">>> [INFO] Numeric Columns ({len(numeric_cols)}): {numeric_cols[:3]}...")
print(f">>> [INFO] String Columns ({len(string_cols)}): {string_cols}")

# ---------------------------------------------------------
# 5. DEFINE PIPELINE STAGES
# ---------------------------------------------------------
stages = []

# STAGE A: String Indexing (Convert Strings -> Numbers)
indexed_string_cols = []
for col_name in string_cols:
    out_col = f"{col_name}_index"
    indexer = StringIndexer(inputCol=col_name, outputCol=out_col, handleInvalid="keep")
    stages.append(indexer)
    indexed_string_cols.append(out_col)

# STAGE B: Imputer (Handle Nulls in Numeric Data only)
imputed_numeric_cols = [f"{c}_imputed" for c in numeric_cols]
imputer = Imputer(
    inputCols=numeric_cols, 
    outputCols=imputed_numeric_cols
).setStrategy("mean")
stages.append(imputer)

# STAGE C: Vector Assembler
# Combine (Indexed Strings) + (Imputed Numerics) into one vector
assembler_inputs = indexed_string_cols + imputed_numeric_cols
assembler = VectorAssembler(
    inputCols=assembler_inputs, 
    outputCol="features_raw"
)
stages.append(assembler)

# STAGE D: Standard Scaler
scaler = StandardScaler(
    inputCol="features_raw", 
    outputCol="features_scaled",
    withStd=True,
    withMean=True
)
stages.append(scaler)

# ---------------------------------------------------------
# 6. EXECUTE PIPELINE
# ---------------------------------------------------------
print(">>> [EXEC] Fitting and Transforming Data...")
pipeline = Pipeline(stages=stages)
model = pipeline.fit(df)
processed_df = model.transform(df)

# ---------------------------------------------------------
# 7. SAVE OUTPUT
# ---------------------------------------------------------
print(f">>> [SAVE] Writing processed data to: {output_path}")

# Write output as Parquet (Compressed & Efficient)
processed_df.select("features_scaled") \
    .write \
    .mode("overwrite") \
    .parquet(output_path)

print(">>> [DONE] Job Completed Successfully.")
spark.stop()
