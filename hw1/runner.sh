#!/usr/bin/env bash


HDFS_LOG_DIR=/user/sandello/logs
HDFS_DATA_DIR=/user/agorokhov/parsed
LOCAL_DATA_DIR=/home/agorokhov/devel/stat
SCRIPT_DIR=$(dirname $0)
CONFIG_DIR=/home/agorokhov/devel/conf

HDFS_COMMAND="hadoop fs"
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

function log_stage {
    echo "++++++ "`date +"%F %T"`" $1"
}

function make_extended_log {
    local date=$1
    if ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/extended/${date}/_SUCCESS >/dev/null 2>&1; then
        return
    fi
    log_stage "Extended ${date}"
    ${HADOOP_STREAM_COMMAND} \
        -files ${SCRIPT_DIR}/parse_access_log.py,${CONFIG_DIR}/IP2LOCATION-LITE-DB1.CSV \
        -D mapreduce.job.reduces=0 \
        -mapper "./parse_access_log.py --geobase IP2LOCATION-LITE-DB1.CSV" \
        -input $(hdfs_log_file ${date}) \
        -output ${HDFS_DATA_DIR}/extended/${date}/ || exit 1
}

function count_sessions {
    local date=$1
    if ! ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/sessions/${date}/_SUCCESS >/dev/null 2>&1; then
        log_stage "Sessions ${date}"
        ${HADOOP_STREAM_COMMAND} \
            -files ${SCRIPT_DIR}/user_sessions.py \
            -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
            -D mapred.text.key.comparator.options=-k1,2 \
            -D stream.num.map.output.key.fields=2 \
            -D mapred.text.key.partitioner.options=-k1,1 \
            -D mapreduce.job.reduces=8 \
            -mapper cat \
            -reducer "./user_sessions.py" \
            -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
            -input ${HDFS_DATA_DIR}/extended/${date} \
            -output ${HDFS_DATA_DIR}/sessions/${date}/ || exit 1
    fi
    if ! ls ${LOCAL_DATA_DIR}/sessions-${date}.txt >/dev/null 2>&1; then
        log_stage "Sessions stat ${date}"
        ${HDFS_COMMAND} -text ${HDFS_DATA_DIR}/sessions/${date}/part* | ${SCRIPT_DIR}/user_sessions.py --sum > ${LOCAL_DATA_DIR}/sessions-${date}.txt.tmp && \
            mv ${LOCAL_DATA_DIR}/sessions-${date}.txt.tmp ${LOCAL_DATA_DIR}/sessions-${date}.txt
    fi
}

function count_pages {
    local date=$1
    if ! ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/pages/${date}/_SUCCESS >/dev/null 2>&1; then
        log_stage "Pages ${date}"
        ${HDFS_COMMAND} -rm -r ${HDFS_DATA_DIR}/pages/${date}.tmp
        ${HADOOP_STREAM_COMMAND} \
            -files ${SCRIPT_DIR}/top_pages.py \
            -D mapreduce.fieldsel.map.output.key.value.fields.spec=2 \
            -D mapreduce.job.reduces=8 \
            -mapper org.apache.hadoop.mapred.lib.FieldSelectionMapReduce \
            -combiner "./top_pages.py -f reducer_sum" \
            -reducer "./top_pages.py -f reducer_sum" \
            -input ${HDFS_DATA_DIR}/extended/${date} \
            -output ${HDFS_DATA_DIR}/pages/${date}.tmp || exit 1

        ${HADOOP_STREAM_COMMAND} \
            -files ${SCRIPT_DIR}/top_pages.py \
            -D mapreduce.job.reduces=0 \
            -mapper "./top_pages.py -f mapper_top" \
            -input ${HDFS_DATA_DIR}/pages/${date}.tmp \
            -output ${HDFS_DATA_DIR}/pages/${date} || exit 1
        ${HDFS_COMMAND} -rm -r ${HDFS_DATA_DIR}/pages/${date}.tmp
    fi
    if ! ls ${LOCAL_DATA_DIR}/pages-${date}.txt >/dev/null 2>&1; then
        log_stage "Pages stat ${date}"
        ${HDFS_COMMAND} -text ${HDFS_DATA_DIR}/pages/${date}/part* | ${SCRIPT_DIR}/top_pages.py -f sort_pages | head -10 > ${LOCAL_DATA_DIR}/pages-${date}.txt.tmp && \
            mv ${LOCAL_DATA_DIR}/pages-${date}.txt.tmp ${LOCAL_DATA_DIR}/pages-${date}.txt
    fi
}


function count_new_users {
    local date=$1
    if ! ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/users/${date}/_SUCCESS >/dev/null 2>&1; then
        log_stage "Users ${date}"
        ${HADOOP_STREAM_COMMAND} \
            -files ${SCRIPT_DIR}/users.py \
            -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
            -D mapred.text.key.comparator.options=-k1,2 \
            -D stream.num.map.output.key.fields=2 \
            -D mapred.text.key.partitioner.options=-k1,1 \
            -D mapreduce.job.reduces=8 \
            -mapper cat \
            -reducer "./users.py -f reducer_day_users" \
            -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
            -input ${HDFS_DATA_DIR}/extended/${date} \
            -output ${HDFS_DATA_DIR}/users/${date} || exit 1
    fi
    if ! ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/new_users/${date}/_SUCCESS >/dev/null 2>&1; then
        log_stage "New users ${date}"
        local input_path=
        for day in {0..13}; do
            d=`date -d "${date} -${day} day" +%F`
            path="${HDFS_DATA_DIR}/users/${d}"
            if ${HDFS_COMMAND} -ls ${path} >/dev/null 2>&1; then
                input_path="${input_path} -input ${path}"
            fi
        done

        ${HADOOP_STREAM_COMMAND} \
            -files ${SCRIPT_DIR}/users.py \
            -D mapreduce.job.reduces=8 \
            -mapper cat \
            -reducer "./users.py -f reducer_new_users --day ${date} --get-users-stat" \
            ${input_path} \
            -output ${HDFS_DATA_DIR}/new_users/${date} || exit 1
    fi
    if ! ls ${LOCAL_DATA_DIR}/new_users-${date}.txt >/dev/null 2>&1; then
        log_stage "New users stat ${date}"
        ${HDFS_COMMAND} -text ${HDFS_DATA_DIR}/new_users/${date}/part* | ${SCRIPT_DIR}/users.py -f count_new_users > ${LOCAL_DATA_DIR}/new_users-${date}.txt.tmp && \
            mv ${LOCAL_DATA_DIR}/new_users-${date}.txt.tmp ${LOCAL_DATA_DIR}/new_users-${date}.txt
    fi
}


function count_facebook_conversion {
    local date=$1
    if ! ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/new_fb_users/${date}/_SUCCESS >/dev/null 2>&1; then
        log_stage "New facebook users ${date}"
        local input_path=
        for day in {0..13}; do
            d=`date -d "${date} -${day} day" +%F`
            path="${HDFS_DATA_DIR}/users/${d}"
            echo "Check ${path}"
            if ${HDFS_COMMAND} -ls ${path} >/dev/null 2>&1; then
                input_path="${input_path} -input ${path}"
                echo "Path ${path} found"
            fi
        done

        ${HADOOP_STREAM_COMMAND} \
            -files ${SCRIPT_DIR}/users.py \
            -D mapreduce.job.reduces=8 \
            -mapper cat \
            -reducer "./users.py -f reducer_new_users --day ${date} --get-new-facebook-users" \
            ${input_path} \
            -output ${HDFS_DATA_DIR}/new_fb_users/${date} || exit 1
    fi

    if ! ${HDFS_COMMAND} -ls ${HDFS_DATA_DIR}/conv_fb_users_2ver/${date}/_SUCCESS >/dev/null 2>&1; then
        log_stage "Converted facebook users ${date}"
        local input_path=
        local mapper_args=
        for day in {0..2}; do
            d=`date -d "${date} -${day} day" +%F`
            path="${HDFS_DATA_DIR}/new_fb_users/${d}"
            if ${HDFS_COMMAND} -ls ${path} >/dev/null 2>&1; then
                input_path="${input_path} -input ${path}"
            fi
        done
        for day in {1..2}; do
            d=`date -d "${date} -${day} day" +%F`
            path="${HDFS_DATA_DIR}/users/${d}"
            if ${HDFS_COMMAND} -ls ${path} >/dev/null 2>&1; then
                input_path="${input_path} -input ${path}"
                mapper_args="${mapper_args} --dataset /users/${d} --tag 2"
            fi
        done
        input_path="${input_path} -input ${HDFS_DATA_DIR}/users/${date}"
        mapper_args="${mapper_args} --dataset /users/${date} --tag 1"

        echo "${mapper_args}"
        echo 0
        ${HADOOP_STREAM_COMMAND} \
            -files ${SCRIPT_DIR}/users.py \
            -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
            -D mapred.text.key.comparator.options=-k1,2 \
            -D stream.num.map.output.key.fields=2 \
            -D mapred.text.key.partitioner.options=-k1,1 \
            -D mapreduce.job.reduces=8 \
            -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
            -mapper "./users.py -f mapper_mark_dataset ${mapper_args}" \
            -reducer "./users.py -f reducer_converted_users --day ${date}" \
            ${input_path} \
            -output ${HDFS_DATA_DIR}/conv_fb_users_2ver/${date} || exit 1
    fi

    if ! ls ${LOCAL_DATA_DIR}/conv_fb_users-${date}.txt >/dev/null 2>&1; then
        log_stage "Converted facebook users stat ${date}"
        ${HDFS_COMMAND} -text ${HDFS_DATA_DIR}/conv_fb_users_2ver/${date}/part* | ${SCRIPT_DIR}/users.py -f count_converted_users > ${LOCAL_DATA_DIR}/conv_fb_users-${date}.txt.tmp && \
            mv ${LOCAL_DATA_DIR}/conv_fb_users-${date}.txt.tmp ${LOCAL_DATA_DIR}/conv_fb_users-${date}.txt
    fi
}


DATE=$1
shift

if [ $(is_log_ready ${DATE}) ]; then
    log_stage "Starting ${DATE}"
else
    exit 1
fi

mkdir -p ${LOCAL_DATA_DIR}

make_extended_log ${DATE}
count_sessions ${DATE}
count_pages ${DATE}
count_new_users ${DATE}
count_facebook_conversion ${DATE}
