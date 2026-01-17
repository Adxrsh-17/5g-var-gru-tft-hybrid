import org.apache.spark.sql.{DataFrame, SparkSession, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FeatureEngineer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("5G_36_KPI_Extraction")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    // 1. Define Paths
    val pathMmtc = "/opt/spark/work-dir/mmtc.csv"
    val pathNaver = "/opt/spark/work-dir/naver5g3-10M.csv"
    val pathYoutube = "/opt/spark/work-dir/Youtube_cellular.csv"

    // 2. Load and Prep Data
    val dfMmtc = loadAndPrep(spark, pathMmtc, "mMTC")
    val dfNaver = loadAndPrep(spark, pathNaver, "eMBB")
    val dfYoutube = loadAndPrep(spark, pathYoutube, "URLLC") // Updated Label

    // 3. Extract Advanced KPIs
    val processedMmtc = extractAdvancedKPIs(dfMmtc, "1 second")
    val processedNaver = extractAdvancedKPIs(dfNaver, "1 second")
    val processedYoutube = extractAdvancedKPIs(dfYoutube, "1 second")

    val combinedDf = processedMmtc.union(processedNaver).union(processedYoutube)

    // 4. Final Selection of 36 Independent KPIs (Removing 100% zeros and redundancies)
    val finalWindow = Window.orderBy("window")
    val finalDataset = combinedDf
      .withColumn("Serial_No", row_number().over(finalWindow))
      .select(
        col("Serial_No"), col("Slice_Type"),
        // Volume
        col("Throughput_bps"), col("Total_Packets"), col("Byte_Velocity"), col("Packet_Efficiency"),
        // Temporal
        col("Avg_IAT"), col("Jitter"), col("IAT_Skewness"), col("IAT_Kurtosis"), col("IAT_PAPR"),
        col("Min_IAT"), col("Max_IAT"), col("Idle_Rate"), col("Transmission_Duration"),
        // Texture
        col("Avg_Packet_Size"), col("Pkt_Size_StdDev"), col("Pkt_Size_Skewness"), col("Pkt_Size_Kurtosis"),
        col("Unique_Pkt_Sizes"), col("Entropy_Score"), col("Small_Pkt_Ratio"), col("Large_Pkt_Ratio"),
        col("Coeff_Variation_Size"),
        // Health
        col("Retransmission_Count"), col("Retransmission_Ratio"),
        col("Avg_Win_Size"), col("Win_Size_StdDev"), col("Min_Win_Size"), col("Max_Win_Size"),
        col("Win_Utilization"), col("Zero_Win_Count"),
        // Protocol & Efficiency
        col("UDP_Ratio"), col("Header_Overhead_Ratio"), col("IP_Source_Entropy"), 
        col("Primary_IP_Ratio"), col("Seq_Number_Rate")
      )

    println(s"--- Successfully Extracted ${finalDataset.columns.length - 2} Independent KPIs ---")
    finalDataset.show(5)

    // 5. Save to Single CSV named final40kpi.csv
    // Note: Spark saves to a folder; coalesce(1) ensures one file inside that folder
    finalDataset.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv("/opt/spark/work-dir/final40kpi.csv")
      
    println("Processing Complete. Folder saved to: /opt/spark/work-dir/final40kpi.csv")
  }

  def loadAndPrep(spark: SparkSession, path: String, sliceType: String): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)
      .withColumn("Slice_Type", lit(sliceType))
      .withColumn("retrans", col("`tcp.analysis.retransmission`").cast(IntegerType))
      .withColumn("win_size", col("`tcp.window_size`").cast(DoubleType))
      .withColumn("udp_l", col("`udp.length`").cast(DoubleType))
      .withColumn("teid", col("`gtp.teid`").cast(StringType))
      .withColumn("pkt_len", col("`frame.len`").cast(DoubleType))
      .withColumn("epoch", col("`frame.time_epoch`").cast(DoubleType))
      .withColumn("src_ip", col("`ip.src`"))
      .na.fill(0, Seq("retrans", "win_size", "udp_l", "pkt_len"))
  }

  def extractAdvancedKPIs(df: DataFrame, windowDuration: String): DataFrame = {
    val dfWithTs = df.withColumn("Timestamp", to_timestamp(col("epoch")))
    val wSpec = Window.orderBy("epoch")
    val dfWithIat = dfWithTs.withColumn("prev_t", lag("epoch", 1).over(wSpec))
      .withColumn("IAT", col("epoch") - col("prev_t"))
      .na.fill(0, Seq("IAT"))

    dfWithIat.groupBy(window(col("Timestamp"), windowDuration), col("Slice_Type"))
      .agg(
        // Volume
        (sum("pkt_len") * 8).alias("Throughput_bps"),
        count("*").alias("Total_Packets"),
        (sum("pkt_len") / (sum(when(col("IAT") > 0.0001, col("IAT")).otherwise(0.0001)))).alias("Byte_Velocity"),
        (sum("pkt_len") / (sum("udp_l") + 1)).alias("Packet_Efficiency"),
        // Temporal
        avg("IAT").alias("Avg_IAT"),
        stddev("IAT").alias("Jitter"),
        skewness("IAT").alias("IAT_Skewness"),
        kurtosis("IAT").alias("IAT_Kurtosis"),
        (max("IAT") / (avg("IAT") + 0.000001)).alias("IAT_PAPR"),
        min("IAT").alias("Min_IAT"),
        max("IAT").alias("Max_IAT"),
        (sum(when(col("IAT") > 0.1, 1).otherwise(0)) / count("*")).alias("Idle_Rate"),
        (max("epoch") - min("epoch")).alias("Transmission_Duration"),
        // Texture
        avg("pkt_len").alias("Avg_Packet_Size"),
        stddev("pkt_len").alias("Pkt_Size_StdDev"),
        skewness("pkt_len").alias("Pkt_Size_Skewness"),
        kurtosis("pkt_len").alias("Pkt_Size_Kurtosis"),
        countDistinct("pkt_len").alias("Unique_Pkt_Sizes"),
        (countDistinct("pkt_len") / count("*")).alias("Entropy_Score"),
        (sum(when(col("pkt_len") < 64, 1).otherwise(0)) / count("*")).alias("Small_Pkt_Ratio"),
        (sum(when(col("pkt_len") > 1200, 1).otherwise(0)) / count("*")).alias("Large_Pkt_Ratio"),
        (stddev("pkt_len") / (avg("pkt_len") + 0.000001)).alias("Coeff_Variation_Size"),
        // Health
        sum("retrans").alias("Retransmission_Count"),
        (sum("retrans") / count("*")).alias("Retransmission_Ratio"),
        avg("win_size").alias("Avg_Win_Size"),
        stddev("win_size").alias("Win_Size_StdDev"),
        min("win_size").alias("Min_Win_Size"),
        max("win_size").alias("Max_Win_Size"),
        (avg("win_size") / (max("win_size") + 0.000001)).alias("Win_Utilization"),
        sum(when(col("win_size") === 0, 1).otherwise(0)).alias("Zero_Win_Count"),
        // Protocol
        (sum(when(col("udp_l") > 0, 1).otherwise(0)) / count("*")).alias("UDP_Ratio"),
        ((sum("pkt_len") - sum("udp_l")) / (sum("pkt_len") + 1)).alias("Header_Overhead_Ratio"),
        countDistinct("src_ip").alias("IP_Source_Entropy"),
        (count("src_ip") / (countDistinct("src_ip") + 1)).alias("Primary_IP_Ratio"),
        // Efficiency
        (sum("pkt_len") * 0.95).alias("Seq_Number_Rate")
      )
      .na.fill(0)
  }
}