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

DEFAULT_JAR=${NMF_HOME}/dist/FinalProject.jar
DEFAULT_K=2
##### LE VEDO?????
##DEFAULT_REMOTE_A=A
##DEFAULT_REMOTE_H=H0
##DEFAULT_REMOTE_W=W0


./HPhases.sh ${DEAFULT_JAR} ${DEFAULT_REMOTE_A} ${DEFAULT_REMOTE_H} ${DEFAULT_REMOTE_W} ${DEFAULT_K}
./WPhases.sh ${DEAFULT_JAR} ${DEFAULT_REMOTE_A} ${DEFAULT_REMOTE_H} ${DEFAULT_REMOTE_W} ${DEFAULT_K}