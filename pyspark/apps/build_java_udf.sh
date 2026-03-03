javac -cp "/opt/spark/jars/*" -d build src/main/java/com/example/*
cd build
jar -cf ../spark-benchmark-udfs-java.jar com/example/*.class

