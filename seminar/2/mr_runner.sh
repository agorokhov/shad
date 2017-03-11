#!/usr/bin/env bash

# Usage:
#
# mr_runner.sh YYYY-MM-DD

HDFS_COMMAND="hadoop fs"
HADOOP_STREAM_COMMAND="hadoop jar /opt/hadoop/hadoop-streaming.jar"

LOCAL_DATA_DIR=/home/${LOGNAME}/mr_runner_out


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

function run_job1 {
    local date=$1
    if ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/out_dir_job1/${date}/_SUCCESS >/dev/null 2>&1; then
        return
    fi
    echo "Running job1 ${date}"
    ${HADOOP_STREAM_COMMAND} \
        -mapper ...
        -input ...
        -output ${HDFS_DATA_DIR}/out_dir_job1/${date}/ || exit 1
        ...
}


# get date from parameter
DATE=$1
shift


# locking with pidfile
mkdir -p ${LOCAL_DATA_DIR}
PIDFILE=${LOCAL_DATA_DIR}/$(basename $0)".pid"
[ -e $PIDFILE ] && kill -0 `cat $PIDFILE` && echo "Already started, pid=`cat $PIDFILE`" && exit 0
echo $$ > $PIDFILE


if [ $(is_log_ready ${DATE}) ]; then
    log_stage "Starting ${DATE}"
else
    rm $PIDFILE
    exit 1
fi

# run jobs
run_job1 ${DATE}


# release lock
rm $PIDFILE
