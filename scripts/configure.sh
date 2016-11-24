#!/bin/bash

echo "Usage: configure.sh master slaves"

### SETUP
export HADOOP_PREFIX=~/java/standalone/hadoop-2.7.2
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
export HADOOP_COMPILATION_CLASSPATH=`yarn classpath`
unset _JAVA_OPTIONS

sed -i -e "s|^export JAVA_HOME=\${JAVA_HOME}|export JAVA_HOME=$JAVA_HOME|g" ${HADOOP_PREFIX}/etc/hadoop/hadoop-env.sh
# Setup yarn Heapsize
sed -i -e "s|^# YARN_HEAPSIZE=1000$|YARN_HEAPSIZE=2000|g" ${HADOOP_PREFIX}/etc/hadoop/yarn-env.sh

MASTER=$1
SLAVES=$@

cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/core-site.xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER}:9000</value>
  </property>
</configuration>
EOF

cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/hdfs-site.xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF

cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/mapred-site.xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

cat <<EOF > ${HADOOP_PREFIX}/etc/hadoop/yarn-site.xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

SLAVES_FILE=${HADOOP_PREFIX}/etc/hadoop/slaves
echo -n "" > $SLAVES_FILE
for s in $SLAVES
  do echo $s >> $SLAVES_FILE
done
