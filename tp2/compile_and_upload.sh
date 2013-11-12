cd flume

echo "Compiling flume"
mvn clean install

echo "Moving flume jar..."
cd target
mv custom-sink-jar-with-dependencies.jar ../../
cd ../../
mv custom-sink-jar-with-dependencies.jar custom-sink.jar

cd storm

echo "Compiling storm"
mvn assembly:assembly

echo "Moving storm jar..."
cd target
mv storm-twitter-0.0.1-SNAPSHOT-jar-with-dependencies.jar ../../
cd ../../
mv storm-twitter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.jar

echo "Uploading twitter-client.jar..."
rsync -auvz -e "ssh -i ../../id_dsa"  twitter-client.jar hadoop@107.20.161.53:/home/hadoop/grupo2/

echo "Uploading custom-sink.jar..."
rsync -auvz -e "ssh -i ../../id_dsa"  custom-sink.jar hadoop@107.20.161.53:/home/hadoop/grupo2/

echo "Uploading storm.jar..."
rsync -auvz -e "ssh -i ../../id_dsa"  storm.jar hadoop@107.20.161.53:/home/hadoop/grupo2/

