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

Once connected, move to grupo2.

In there, there should be the following files and directories:

1. apache-flume-1.4.0-SNAPSHOT-bin (dir)
2. flume.conf (flume configuration file)
3. logs
4. tweets (file where tweets files are stored)
5. twitter-client.jar
6. custom-sink.jar

If you want to run twitter client and then process those files with flume, execute the following:

```
java -jar twitter-client.jar
./apache-flume-1.4.0-SNAPSHOT-bin/bin/flume-ng agent -n a1 -c apache-flume-1.4.0-SNAPSHOT-bin/conf/ -f flume.conf -C custom-sink.jar
```

## Flume

1- After you download flume

```
$ cp flume-ng-dist/target/apache-flume-1.4.0-SNAPSHOT-bin.tar.gz .

$ tar -zxvf apache-flume-1.4.0-SNAPSHOT-bin.tar.gz
```
2- To run using flume.conf file located in config directory

```
./apache-flume-1.4.0-SNAPSHOT-bin/bin/flume-ng agent -n a1 -c apache-flume-1.4.0-SNAPSHOT-bin/conf/ -f flume.conf
```

If you want to debug, set the sink to be logger and add the following line to the previous:
```
-Dflume.root.logger=INFO,console
```
