#!/usr/bin/env bash


SOURCE_PATH=/Users/we/wmp_git/python3-10-0-study/source/delta_streaming_test/streaming_test.py
SPARK_HOME=/Users/we/wmp_git/python3-10-0-study/venv/lib/python3.10/site-packages/pyspark
cd ${SOURCE_DIRECOTRY}

${SPARK_HOME}/bin/spark-submit \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.sql.crossJoin.enabled=true \
--conf spark.streaming.stopGracefullyOnShutdown=true \
--conf spark.sql.parquet.enableVectorizedReader=false \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--packages org.apache.spark:spark-avro_2.12:3.0.2,io.delta:delta-core_2.12:1.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
--master local[*] \
--deploy-mode client \
${SOURCE_PATH}


if [ $? -eq 0 ]
then
  echo "Success"
else
  echo "Error"
  exit 1
fi
