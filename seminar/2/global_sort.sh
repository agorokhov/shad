# step 1
hadoop --config conf.empty jar /opt/hadoop/hadoop-streaming.jar \
    -files get_url.py -input log_url/ -output out_sort_tmp \
    -mapper ./get_url.py \
    -reducer ./hits_per_id.py

# sort
hadoop --config conf.empty jar /opt/hadoop/hadoop-streaming.jar \
    -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D mapred.text.key.comparator.options=-nr \
    -files inverse.py -input out_sort_tmp/ -output out_sort/ \
    -mapper inverse.py \
    -reducer inverse.py

