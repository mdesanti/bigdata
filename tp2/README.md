# TP2

First connect to the cluster:

`ssh -i id_dsa hadoop@107.20.161.53`

Once connected, move to grupo2.

In there, there should be the following files and directories:

1. apache-flume-1.4.0-SNAPSHOT-bin (dir)
2. flume.conf (flume configuration file)
3. logs
4. tweets (file where tweets files are stored)
5. twitter-client.jar

If you want to run twitter client and then process those files with flume, execute the following:

```
java -jar twitter-client.jar
./apache-flume-1.4.0-SNAPSHOT-bin/bin/flume-ng agent -n a1 -c apache-flume-1.4.0-SNAPSHOT-bin/conf/ -f flume.conf
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
