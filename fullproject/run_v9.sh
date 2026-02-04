#!/bin/bash
# Use DNS-resolvable hostnames (containers renamed: bd_namenode -> namenode, bd_kafka -> kafka)
export HDFS_NAMENODE="hdfs://namenode:8020"
export KAFKA_BOOTSTRAP="kafka:9092"

/opt/spark/bin/spark-submit \
  --class KafkaKpiPipeline \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 4G \
  --executor-memory 2G \
  --num-executors 1 \
  --conf "spark.driver.extraJavaOptions=-DHDFS_NAMENODE=hdfs://namenode:8020" \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  --conf spark.sql.streaming.checkpointLocation=hdfs://namenode:8020/tmp/checkpoint_v9 \
  /tmp/5g-kpi-pipeline-assembly-1.0.jar > /tmp/streaming_v9.log 2>&1
