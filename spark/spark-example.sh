#!/bin/sh
spark-submit --class org.apache.spark.examples.SparkPi --name SparkPI --master yarn-cluster --executor-cores 2 --executor-memory 256m /opt/cloudera/parcels/CDH/jars/spark-examples*.jar 900
