EB_V=2.2.0

javac -classpath /usr/lib/hadoop/hadoop-core.jar:/project/hanna/src/elephant-bird/build/elephant-bird-${EB_V}.jar \
-d classes LzoJsonWordCount.java 

jar -cvf LzoJsonWordCount.jar -C classes/ .
