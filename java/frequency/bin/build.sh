javac -cp /usr/lib/hadoop/hadoop-core.jar:/project/hanna/src/java:/project/hanna/src/java/gson-2.2.jar:. -d frequency_classes Frequency.java
jar -cvf frequency.jar -C frequency_classes .
