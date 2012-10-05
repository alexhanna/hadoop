hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u3.jar \
    -input /user/ahanna/jan25/jan25.json \
    -output /user/ahanna/output \
    -mapper filter.py \
#    -reducer org.apache.hadoop.mapred.lib.IdentityMapper \
    -reducer wc -l \
    -file /home/a/ahanna/sandbox/hadoop/filter/streaming/filter.py \
    -file /home/a/ahanna/sandbox/hadoop/filter/streaming/twitpak.mod
