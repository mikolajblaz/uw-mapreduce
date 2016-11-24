#!/bin/bash

echo "Usage: compilenrun.sh classname args"

PREFIX=$1
shift

javac -classpath "$HADOOP_COMPILATION_CLASSPATH" -sourcepath . -d . $PREFIX.java

if [ $? -eq 0 ]
then
	jar cf $PREFIX.jar *.class
	yarn jar $PREFIX.jar $PREFIX $@
else
	echo "COMPILATION ERROR"
fi

