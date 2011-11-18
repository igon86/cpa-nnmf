#!/bin/bash

##first parameter: jar file
##second parameter: A Files
##third parameter: H Files
##fourth parameter: W Files
##fiveth parameter: factorizing parameter

###### GENERIC VARIABLE ######
JAR_NAME=$1
accepted_parameters=6
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
	echo "sixthth parameter: iteration number"
	exit
fi

###### DATA INPUT FILES ######
iter_numb=$6
A_DATA=$2
H_DATA=${3}$((iter_numb-1))
W_DATA=${4}$((iter_numb-1))
K_DIM=$5
X_PARTIAL=X_PARTIALw$6
X_FINAL=X_FINALw$6
Y_FINAL=Y_FINALw$6
C_DATA=C_DATAw$6
W_PRIME=W$((iter_numb++))


###### STARTING H PHASE ######

###### COMPUTING X ######
echo %%%%PHASE 1%%%%%%
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} WPhaseSequence.WPhase1 $A_DATA $H_DATA $X_PARTIAL $K_DIM
echo %%%%PHASE 2%%%%%%
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase2 $X_PARTIAL $X_FINAL $K_DIM

###### COMPUTING Y ######
echo %%%%PHASE 3%%%%%%
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase3 $H_DATA $C_DATA $K_DIM
echo %%%%PHASE 4%%%%%%
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} WPhaseSequence.WPhase4 $W_DATA $C_DATA $Y_FINAL $K_DIM

###### COMPUTING THE UPDATED MATRIX W ######
echo %%%%PHASE 5%%%%%%
${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} HPhaseSequence.HPhase5 $W_DATA $X_FINAL $Y_FINAL $W_PRIME $K_DIM
