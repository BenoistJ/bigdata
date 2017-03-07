#!/bin/sh
mkdir /tmp/spark-events
hadoop fs -mkdir /tmp/spark
hadoop fs -put jars/* /tmp/spark
