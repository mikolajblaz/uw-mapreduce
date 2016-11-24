#!/bin/bash

hdfs dfs -mkdir /input

hdfs dfs -put $@ /input
