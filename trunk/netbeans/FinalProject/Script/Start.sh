#!/bin/bash

${NMF_HOME}/Script/EraseAll.sh

${HADOOP_HOME}/bin/start-all.sh

echo ""
echo "Wait until the web interface is fully up."
echo "Check http://<hdfs_host>:50070/dfshealth.jsp"
echo "Check http://<track_host>:50030/jobtracker.jsp"
