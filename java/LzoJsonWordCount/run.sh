HADOOP_HOME=/user/ahanna
EB_V=2.2.0

hadoop dfs -rmr ${HADOOP_HOME}/output

hadoop jar LzoJsonWordCount.jar org.ahanna.LzoJsonWordCount \
-libjars /project/hanna/src/elephant-bird/build/elephant-bird-${EB_V}.jar \
${HADOOP_HOME}jan25/jan25.json.lzo ${HADOOP_HOME}/output
