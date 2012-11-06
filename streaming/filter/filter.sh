hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=2 \
    -D map.output.key.field.separator="\t" \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/filter/filterMap.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/filter/filterReduce.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/latinKeywords.txt \
    -input /user/ahanna/elex2012 \
    -output /user/ahanna/output \
    -mapper filterMap.py \
    -reducer filterReduce.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner 
