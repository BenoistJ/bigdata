#!/bin/sh

#$HIVE_HOME/bin/hive
clear
nohup $HIVE_HOME/bin/hiveserver2 --hiveconf hive.root.logger=INFO,console &
