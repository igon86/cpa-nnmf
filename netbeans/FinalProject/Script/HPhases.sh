#!/bin/bash

## first parameter: jar file
## second parameter: A File
## third parameter: H Files

###### GENERIC VARIABLE ######
JAR_NAME=Project.jar
accepted_parameters=4
printWorkingDir=$(pwd)

###### CHECKING PARAMETERS ######
if [[ $# -ne $accepted_parameters ]]
then
	echo "The number of parameters are not corrected"
	echo "first parameter: jar file"
	echo "second parameter: A Files"
	echo "third parameter: H Files"
	echo "fourth parameter: W Files"
	exit
fi

###### DATA INPUT FILES ######
A_DATA=$2
H_DATA=$3
W_DATA=$4
X_PARTIAL=X_PARTIAL
X_FINAL=X_FINAL
C_DATA=C_DATA


###### STARTING H PHASE ######
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhase.HPhase1 $A_DATA $W_DATA X_PARTIAL
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhase.HPhase2 X_PARTIAL X_FINAL

${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhase.HPhase3 W_DATA C_DATA
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhase.HPhase4 H_DATA C_DATA Y_FINAL

${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhase.HPhase5 H_DATA X_FINAL Y_FINAL

###cd ${HADOOP_HOME}/bin
##file1=$(dirname $1)
###echo "##### $file1"

##. ${HADOOP_HOME}/bin/hadoop
###. hadoop fs -ls

###echo $1 $2 $3
