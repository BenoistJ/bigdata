#!/usr/bin/env bash
clear

spark-submit \
  --master spark://mapr1:7077 \
  --class Rdd \
  target/scala-2.10/benoist.jar

