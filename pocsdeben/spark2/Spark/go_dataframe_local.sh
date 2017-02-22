#!/usr/bin/env bash
clear

spark-submit \
  --master local[4] \
  --jars /home/ubuntu/.ivy2/cache/com.databricks/spark-csv_2.10/jars/spark-csv_2.10-1.3.0.jar,/home/ubuntu/.ivy2/cache/org.apache.commons/commons-csv/jars/commons-csv-1.1.jar \
  target/scala-2.10/benoist.jar

