javac -cp /usr/lib/hadoop/hadoop-core.jar:/project/hanna/src/java:/project/hanna/src/java/gson-2.2.jar:. -d datecount_classes DateCount.java
jar -cvf datecount.jar -C datecount_classes .
