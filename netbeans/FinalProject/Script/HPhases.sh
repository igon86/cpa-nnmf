#!/bin/bash

##first parameter: jar file
##second parameter: A Files
##third parameter: H Files
##fourth parameter: W Files
##fiveth parameter: factorizing parameter

###### GENERIC VARIABLE ######
JAR_NAME=$1
accepted_parameters=5
##printWorkingDir=$(pwd)

###### CHECKING PARAMETERS ######
if [[ $# -ne $accepted_parameters ]]
then
	echo "The number of parameters are not corrected"
	echo "first parameter: jar file"
	echo "second parameter: A Files"
	echo "third parameter: H Files"
	echo "fourth parameter: W Files"
	echo "fiveth parameter: factorizing parameter"
	exit
fi

###### DATA INPUT FILES ######
A_DATA=$2
H_DATA=$3
W_DATA=$4
K_DIM=$5
X_PARTIAL=X_PARTIAL
X_FINAL=X_FINAL
C_DATA=C_DATA
H_PRIME=H_PRIME


###### STARTING H PHASE ######

###### COMPUTING X ######
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase1 $A_DATA $W_DATA $X_PARTIAL $K_DIM
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase2 $X_PARTIAL $X_FINAL $K_DIM

###### COMPUTING Y ######
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase3 $W_DATA $C_DATA $K_DIM
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase4 $H_DATA $C_DATA $Y_FINAL $K_DIM

###### COMPUTING THE UPDATED MATRIX H ######
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase5 $H_DATA $X_FINAL $Y_FINAL $H_PRIME $K_DIM
