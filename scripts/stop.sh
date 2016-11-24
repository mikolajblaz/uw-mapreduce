#!/bin/bash

### STOP
${HADOOP_PREFIX}/sbin/stop-yarn.sh
${HADOOP_PREFIX}/sbin/stop-dfs.sh

for slave in `cat ${HADOOP_PREFIX}/etc/hadoop/slaves`
  do ssh $slave 'rm -rf /tmp/*'
done