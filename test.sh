#!/bin/bash

diff -r output $1
if [ 0 -ne $? ]
then echo "ERRORRRRRRRRRRRRRRRR"
else echo "OK"
fi
