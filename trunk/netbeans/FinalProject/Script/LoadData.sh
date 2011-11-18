#!/bin/bash

## first parameter: A source directory
## second parameter: A destination directory
## third parameter: H source directory
## fourth parameter: H destination directory
## fiveth parameter: W source directory
## sixth parameter: W destination directory

###### GENERIC VARIABLE ######
accepted_parameters=6
##printWorkingDir=$(pwd)

###### CHECKING PARAMETERS ######
if [[ $# -ne $accepted_parameters ]]
then
	echo "first parameter: A source directory"
	echo "second parameter: A destination directory"
	echo "third parameter: H source directory"
	echo "fourth parameter: H destination directory"
	echo "fiveth parameter: W source directory"
	echo "sixth parameter: W destination directory"
	exit
fi

echo 1#$1
echo 2#$2
echo 3#$3
echo 4#$4
echo 5#$5
echo 6#$6

###### COPYING THE A DATA ######
#${HADOOP_HOME}/bin/hadoop fs -rmr $2
${HADOOP_HOME}/bin/hadoop fs -mkdir $2
${HADOOP_HOME}/bin/hadoop fs -put ${1}/*.data ${2}/

###### COPYING THE H DATA ######
#${HADOOP_HOME}/bin/hadoop fs -rmr $4
${HADOOP_HOME}/bin/hadoop fs -mkdir $4
${HADOOP_HOME}/bin/hadoop fs -put ${3}/*.data ${4}/

###### COPYING THE W DATA ######
#${HADOOP_HOME}/bin/hadoop fs -rmr $6
${HADOOP_HOME}/bin/hadoop fs -mkdir $6
${HADOOP_HOME}/bin/hadoop fs -put ${5}/*.data ${6}/