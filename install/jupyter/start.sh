#!/bin/sh
PYSPARK_DRIVER_PYTHON='jupyter' PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip='bigdata1' --port=8889 --allow-root --no-browser' MASTER='yarn' pyspark --jars pyspark-cassandra-0.3.5.jar,spark-cassandra-connector-2.0.0-M2-s_2.11.jar --py-files pyspark-cassandra-0.3.5.jar
