#!/bin/bash

##first parameter: jar file
##second parameter: A Files
##third parameter: H Files
##fourth parameter: W Files
##fiveth parameter: factorizing parameter

###### GENERIC VARIABLE ######
JAR_NAME=$1
accepted_parameters=6
one=1
##printWorkingDir=$(pwd)

###### CHECKING PARAMETERS ######

if [[ $# -ne $accepted_parameters ]]
then
	echo number of parameters $#
	if [[ $# -eq $one ]]
	then
		iter_numb=$1
		DEFAULT_REMOTE_A=A0
		DEFAULT_REMOTE_H=H
		DEFAULT_REMOTE_W=W
		export DEFAULT_JAR=${NMF_HOME}/dist/FinalProject.jar

	else
		echo "The number of parameters are not corrected"
		echo "first parameter: jar file"
		echo "second parameter: A Files"
		echo "third parameter: H Files"
		echo "fourth parameter: W Files"
		echo "fiveth parameter: factorizing parameter"
		exit
	fi
fi

DEFAULT_K=2
##### LE VEDO?????


for((iter=1;iter<=${iter_numb};iter++));do
	${NMF_HOME}/Script/HPhases.sh ${DEFAULT_JAR} ${DEFAULT_REMOTE_A} ${DEFAULT_REMOTE_H} ${DEFAULT_REMOTE_W} ${DEFAULT_K} ${iter}
	${NMF_HOME}/Script/WPhases.sh ${DEFAULT_JAR} ${DEFAULT_REMOTE_A} ${DEFAULT_REMOTE_H} ${DEFAULT_REMOTE_W} ${DEFAULT_K} ${iter}
done