hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/getTweets/getTweets.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/follow-r1.txt \
    -input elex2012/* \
    -output output \
    -mapper getTweets.py \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer 
