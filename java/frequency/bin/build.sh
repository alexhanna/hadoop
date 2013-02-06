javac -cp /usr/lib/hadoop-mapreduce/*.jar:/usr/lib/hadoop/*.jar:../../../jar/*.jar:. -d frequency_classes Frequency.java
jar -cvf frequency.jar -C frequency_classes .
