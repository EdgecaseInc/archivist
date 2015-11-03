#!/bin/bash
sudo aws s3 cp s3://platform-configuration/custom.jar /usr/lib/hadoop/lib/custom.jar
#export HADOOP_USER_CLASSPATH_FIRST=true
#echo "HADOOP_CLASSPATH=/home/hadoop/custom.jar" >> /home/hadoop/conf/hadoop-user-env.sh
#hadoop fs -copyToLocal s3n://platform-configuration/custom.jar /usr/lib/hadoop/lib/custom.jar
