#!/bin/bash +vx
LIB_PATH=lib/protobuf-java-3.7.1.jar

# java -classpath bin/:$LIB_PATH Branch $1 $2

for (( num=1; num <= $3; num++ ))
do
    java -classpath bin/:$LIB_PATH Branch $1$num $(($2+$num)) &
done