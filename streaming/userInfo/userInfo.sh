hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=4 \
    -D map.output.key.field.separator="\t" \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/userInfo/userInfo.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/userInfo/userInfoReduce.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/follow-r1.txt \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/follow-r2.txt \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/follow-r3.txt \
    -input /user/ahanna/elex-wave3 \
    -output /user/ahanna/output \
    -mapper userInfo.py \
    -reducer userInfoReduce.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner 

#    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \

