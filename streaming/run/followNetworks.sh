hdfs dfs -rm -r /user/ahanna/output
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $HOME/sandbox/hadoop/streaming/map/findFollowNetworks.py,$HOME/sandbox/hadoop/streaming/data/follow-r1.txt,$HOME/sandbox/hadoop/streaming/data/follow-r2.txt,$HOME/sandbox/hadoop/streaming/data/follow-r3.txt \
    -input elex2012net/relation-r1.txt \
    -output output \
    -mapper "findFollowNetworks.py -l follow-r1.txt" \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
    -numReduceTasks 1