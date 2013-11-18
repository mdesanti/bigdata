# TP2

## Compiling and uploading to cluster

To compile and upload the custom sink, storm and other necessary files, execute:

```
sh compile_and_upload.sh
```

IMPORTANT: the script assumes that the id_dsa file is in ../../ (out of the repository, for security reasons). Either you
place it there or you modify the script to update the id_dsa file location.

First connect to the cluster:

`ssh -i id_dsa hadoop@107.20.161.53`

Once connected, move to grupo2 (this folder is not in HDFS!).

`cd grupo2`

In there, there should be the following files and directories:

1. apache-flume-1.4.0-SNAPSHOT-bin (dir)
2. flume.conf (flume configuration file)
3. logs
4. tweets (file where tweets files are stored)
5. twitter-client.jar
6. custom-sink.jar
7. storm.jar

#Flume

If you want to run twitter client and then process those files with flume, execute the following:

```
java -jar twitter-client.jar
./apache-flume-1.4.0-SNAPSHOT-bin/bin/flume-ng agent -n a1 -c apache-flume-1.4.0-SNAPSHOT-bin/conf/ -f flume.conf -C custom-sink.jar
```

Or just copy the files to process to /tweets and execute the following:

```
./apache-flume-1.4.0-SNAPSHOT-bin/bin/flume-ng agent -n a1 -c apache-flume-1.4.0-SNAPSHOT-bin/conf/ -f flume.conf -C custom-sink.jar
```

#Storm

Storm must be run from datanode 4. To do so, copy storm to datanode 4 executing:
`scp storm.jar hadoop-2013-datanode-4:grupo2`.

Afterwards, move to datanode 4: `ssh hadoop-2013-datanode-4`. Run the storm with the following command:
`storm-0.8.2/bin/storm jar grupo2/storm.jar topology.Topology <storm_name>`

If you get an error saying somthing like 'connection refused...' then probably nimbus is not running. To start nimbus:

`storm nimbus`

In addition, nimbus ui might not be activated. If so, `storm ui`. This ui can be found [here](http://50.19.65.50:8080/)

If you are developing at ITBA, then the proxy is probably blocking your connection to the UI. Make an ssh tunnel to
the UI using the following:

`ssh -i id_dsa -L 9000:10.242.58.110:8080 hadoop@107.20.161.53`

./execute_in_some.sh "nohup ~/storm-0.8.2/bin/storm supervisor 2> /dev/null &"

#MySQL


Results processed in storm are stored in a table in sql.

The table is called `party` and has a column `name` with the party name and a column `quantity` with the quantity of matches for that party.

To get the final statisticts just run the following query:

`select name, quantity from party;`

