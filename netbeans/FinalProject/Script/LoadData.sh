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

###### COPYING THE A DATA ######
${HADOOP_HOME}/bin/hadoop fs -rmr $2
${HADOOP_HOME}/bin/hadoop fs -mkdir $2
${HADOOP_HOME}/bin/hadoop fs -cp ${1}/* ${2}

###### COPYING THE H DATA ######
${HADOOP_HOME}/bin/hadoop fs -rmr $4
${HADOOP_HOME}/bin/hadoop fs -mkdir $4
${HADOOP_HOME}/bin/hadoop fs -cp ${3}/* ${4}

###### COPYING THE W DATA ######
${HADOOP_HOME}/bin/hadoop fs -rmr $6
${HADOOP_HOME}/bin/hadoop fs -mkdir $6
${HADOOP_HOME}/bin/hadoop fs -cp ${5}/* ${6}