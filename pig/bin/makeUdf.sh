javac -cp ../../jar/pig.jar:../../jar/json-simple-1.1.1.jar:../../jar/elephant-bird-pig-3.0.0.jar ../myudfs/*.java
cd ..
jar -cf myudfs.jar myudfs
mv myudfs.jar ../jar
