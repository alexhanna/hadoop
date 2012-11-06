javac -classpath /usr/lib/hadoop/hadoop-core.jar:/project/hanna/src/elephant-bird/build/elephant-bird-2.2.0.jar -d classes LzoJsonWordCount.java 
jar -cvf LzoJsonWordCount.jar -C classes/ .
