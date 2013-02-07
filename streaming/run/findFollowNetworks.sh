hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/findFollowNetworks.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/data/follow.txt \
    -input /user/ahanna/elex2012net \
    -output /user/ahanna/output \
    -mapper findFollowNetworks.py \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer 
