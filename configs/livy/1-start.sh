#!/bin/sh


#https://blogs.msdn.microsoft.com/pliu/2016/06/18/run-hue-spark-notebook-on-cloudera/
#wget http://archive.cloudera.com/beta/livy/livy-server-0.3.0.zip
#unzip livy-server-0.3.0.zip

export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export HADOOP_CONF_DIR=/run/cloudera-scm-agent/process/383-hue-HUE_SERVER/yarn-conf
export HUE_SECRET_KEY=N...1

./bin/livy-server &
