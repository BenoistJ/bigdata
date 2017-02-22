#!/usr/bin/env bash
clear

#git pull
sbt package

spark-submit \
  --master spark://mapr1:7077 \
  --class PrepareData \
  target/scala-2.10/benoist.jar
