cd flume

echo "Compiling flume"
mvn install > /dev/null

echo "Moving flume jar..."
cd target
mv custom-sink-jar-with-dependencies.jar ../..
cd ../..
mv custom-sink-jar-with-dependencies.jar custom-sink.jar

cd storm

echo "Compiling storm"
mvn install > /dev/null

echo "Moving storm jar..."
cd target
mv storm-twitter-0.0.1-SNAPSHOT-jar-with-dependencies.jar ../../storm-twitter.jar
cd ../..
mv storm-twitter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm-twitter.jar

echo "Uploading twitter-client.jar..."
rsync -v --rsh=ssh twitter-client.jar hadoop-2013-namenode:/home/hadoop/grupo2/

echo "Uploading custom-sink.jar..."
rsync -v --rsh=ssh custom-sink.jar hadoop-2013-namenode:/home/hadoop/grupo2/

echo "Uploading storm.jar..."
rsync -v --rsh=ssh storm-twitter.jar hadoop-2013-namenode:/home/hadoop/grupo2/

