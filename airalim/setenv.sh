#!/usr/bin/env bash
clear
export LD_LIBRARY_PATH=/home/benoist/hadoop-2.7.3/lib/native/:$LD_LIBRARY_PATH

export SPARK_HOME=/home/benoist/spark-2.0.2-bin-hadoop2.7
source "${SPARK_HOME}"/bin/load-spark-env.sh
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.3-src.zip:$PYTHONPATH"
export SPARK_LOCAL_IP=192.168.1.100

python loadTest.py
