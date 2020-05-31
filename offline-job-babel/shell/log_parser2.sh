#!/bin/bash
source ~/.bashrc

if [ $# -lt 2 ]; then
    echo "sh log_parser.sh <date> <className>"
    exit 1
fi

# xxx部分需要自己指定.

cluster=yarn

date=${1}

className=${2}
output_path=xxx

jarPath="offline-job-babel-1.0-SNAPSHOT.jar"
if [ ! -f "offline-job-babel-1.0-SNAPSHOT.jar" ]; then
    jarPath="../target/offline-job-babel-1.0-SNAPSHOT.jar"
else
    echo "-----offline-job-babel-1.0-SNAPSHOT.jar exist!-----"
fi

echo "-------------------job info-----------------------"
echo "You are running jar at path:" ${jarPath}
echo "job work for date:" ${date}
echo "className:" ${className}
echo "-------------------job start!-----------------------"

hadoop fs -rm -r -skipTrash ${output_path}

spark-submit --cluster ${cluster} \
    --master yarn \
    --deploy-mode cluster \
    --class com.offline.job.babel.log.LogParseEngine \
    ${jarPath} ${output_path} $*

echo "hadoop fs -cat" ${output_path}"/*" "| less"