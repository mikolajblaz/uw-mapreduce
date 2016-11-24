#!/bin/bash

### START
${HADOOP_PREFIX}/bin/hdfs namenode -format

${HADOOP_PREFIX}/sbin/start-dfs.sh

${HADOOP_PREFIX}/bin/hdfs dfs -mkdir /user
${HADOOP_PREFIX}/bin/hdfs dfs -mkdir /user/mb346862

${HADOOP_PREFIX}/sbin/start-yarn.sh

./put_to_hdfs.sh ../input/*
