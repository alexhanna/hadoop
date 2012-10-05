javac -cp /usr/lib/hadoop/hadoop-core.jar:/project/hanna/src/java:/project/hanna/src/java/elephant-bird-2.2.0.jar:. -d classes LzoJsonWordCount.java
jar -cvf lzojsoncount.jar -C classes .
