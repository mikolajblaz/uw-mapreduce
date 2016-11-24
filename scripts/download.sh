#!/bin/bash

### DOWNLOAD
wget -P ~/Pobrane ftp://ftp.task.gda.pl/pub/www/apache/dist/hadoop/core/hadoop-2.7.2/hadoop-2.7.2.tar.gz

mkdir -p ~/java/standalone

tar -xvf ~/Pobrane/hadoop-2.7.2.tar.gz -C ~/java/standalone/

rm -f ~/Pobrane/hadoop-2.7.2.tar.gz

export HADOOP_PREFIX=~/java/standalone/hadoop-2.7.2
