#!/bin/bash
export HDFS_NAMENODE="hdfs://172.18.0.4:8020"
export KAFKA_BOOTSTRAP="172.18.0.9:9092"

/opt/spark/bin/spark-submit \
  --class KafkaKpiPipeline \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 4G \
  --executor-memory 2G \
  --num-executors 1 \
  --conf "spark.driver.extraJavaOptions=-DHDFS_NAMENODE=hdfs://172.18.0.4:8020" \
  --conf spark.hadoop.fs.defaultFS=hdfs://172.18.0.4:8020 \
  --conf spark.sql.streaming.checkpointLocation=hdfs://172.18.0.4:8020/tmp/checkpoint_v9 \
  /tmp/5g-kpi-pipeline-v6.jar > /tmp/streaming_v9.log 2>&1
