#!/bin/bash
clear
spark-submit \
	--jars pyspark-cassandra-0.3.5.jar,spark-cassandra-connector_2.10-2.0.0-M3.jar,scala-reflect-2.10.5.jar \
    --py-files pyspark-cassandra-0.3.5.jar \
    mmm.py


#clear;pyspark --packages TargetHolding/pyspark-cassandra:0.3.5 --conf spark.cassandra.connection.host=192.168.1.100 --master spark://127.0.1.1:7077
#import pyspark_cassandra