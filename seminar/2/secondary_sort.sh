# secondary sort
hadoop --config conf.empty jar /opt/hadoop/hadoop-streaming.jar \
    -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D mapred.text.key.comparator.options=-k1,2 \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.text.key.partitioner.options=-k1,1 \
    -D mapreduce.job.reduces=3 \
    -files get_user_id_timestamp_re.py -input log_sessions/ -output out/ \
    -mapper ./get_user_id_timestamp_re.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -reducer cat

