cd flume

echo "Compiling flume"
mvn clean install

echo "Moving jar..."
cd target
mv custom-sink-jar-with-dependencies.jar ../../
cd ../../
mv custom-sink-jar-with-dependencies.jar custom-sink.jar

echo "Uploading twitter-client.jar..."
scp -i ../../id_dsa twitter-client.jar hadoop@107.20.161.53:./grupo2/

echo "Uploading custom-sink.jar..."
scp -i ../../id_dsa custom-sink.jar hadoop@107.20.161.53:./grupo2/

