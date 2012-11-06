hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=2 \
    -D map.output.key.field.separator="\t" \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/net/categorizeByNet.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/net/categorizeReduce.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/follow-r1.txt \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/top-level-uid_cat.csv \
    -input /user/ahanna/elex2012net \
    -output /user/ahanna/output \
    -mapper categorizeByNet.py \
    -reducer categorizeReduce.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner 
