hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u3.jar \
    -input /user/ahanna/elex2012
    -output /user/ahanna/output \
    -mapper filter.py \
    -reducer reducer.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/filter.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/reducer.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/twitpak.mod
