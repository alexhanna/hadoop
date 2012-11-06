hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -libjars /project/hanna/src/elephant-bird/build/elephant-bird-2.2.0.jar,/project/hanna/src/guava-13.0.1.jar,/project/hanna/src/elephant-bird/lib/hadoop-lzo-0.4.15.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=2 \
    -D map.output.key.field.separator="\t" \
    -D mapred.text.key.partitioner.options=-k1,2 \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/filter/filterMap.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/filter/filterReduce.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/latinKeywords.txt \
    -inputformat com.twitter.elephantbird.mapred.input.DeprecatedLzoTextInputFormat \
    -input /user/ahanna/lzotest \
    -output /user/ahanna/output \
    -mapper filterMap.py \
    -reducer filterReduce.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner 
