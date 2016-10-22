#!/usr/bin/env bash


HDFS_LOG_DIR=/user/sandello/logs
HDFS_COMMAND="hadoop fs"
HDFS_DATA_DIR=/user/agorokhov/parsed
LOCAL_DATA_DIR=/home/agorokhov/devel/1/stat

HADOOP_STREAM_COMMAND="hadoop jar /opt/hadoop/hadoop-streaming.jar"


function hdfs_log_file {
    local date=$1
    echo "${HDFS_LOG_DIR}/access.log.${date}"
}

function is_log_ready {
    local date=$1
    local date_next=`date -d "${date} +1 day" +%F`

    ${HDFS_COMMAND} -ls $(hdfs_log_file ${date}) >/dev/null 2>&1 && \
        ${HDFS_COMMAND} -ls $(hdfs_log_file ${date_next}) >/dev/null 2>&1 && echo "1"
}

function make_extended_log {
    local date=$1
    if ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/extended/${date}/_SUCCESS >/dev/null 2>&1; then
        return
    fi
    echo "Extended ${date}"
    ${HADOOP_STREAM_COMMAND} \
        -files parse_access_log.py,IP2LOCATION-LITE-DB1.CSV \
        -D mapreduce.job.reduces=0 \
        -mapper "./parse_access_log.py --geobase IP2LOCATION-LITE-DB1.CSV" \
        -input $(hdfs_log_file ${date}) \
        -output ${HDFS_DATA_DIR}/extended/${date}/
}

function count_sessions {
    local date=$1
    if ! ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/sessions/${date}/_SUCCESS >/dev/null 2>&1; then
        echo "Sessions ${date}"
        ${HADOOP_STREAM_COMMAND} \
            -files user_sessions.py \
            -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
            -D mapred.text.key.comparator.options=-k1,2 \
            -D stream.num.map.output.key.fields=2 \
            -D mapred.text.key.partitioner.options=-k1,1 \
            -D mapreduce.job.reduces=8 \
            -mapper cat \
            -reducer "./user_sessions.py" \
            -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
            -input ${HDFS_DATA_DIR}/extended/${date} \
            -output ${HDFS_DATA_DIR}/sessions/${date}/
    fi
    if ! ls ${LOCAL_DATA_DIR}/sessions-${date}.txt >/dev/null 2>&1; then
        echo "Sessions stat ${date}"
        ${HDFS_COMMAND} -text ${HDFS_DATA_DIR}/sessions/${date}/part* | ./user_sessions.py --sum > ${LOCAL_DATA_DIR}/sessions-${date}.txt.tmp && \
            mv ${LOCAL_DATA_DIR}/sessions-${date}.txt.tmp ${LOCAL_DATA_DIR}/sessions-${date}.txt
    fi
}

DATE=$1
shift

if [ $(is_log_ready ${DATE}) ]; then
    echo "Starting ${DATE}"
else
    exit 1
fi

mkdir -p ${LOCAL_DATA_DIR}

make_extended_log ${DATE}
count_sessions ${DATE}
