javac -cp /usr/lib/hadoop/hadoop-core.jar:/project/hanna/src/java:/project/hanna/src/java/gson-2.2.jar:. -d network_classes Network.java
jar -cvf network.jar -C network_classes .
