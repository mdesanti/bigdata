## Flume

1. After you download flume

```
  $ cp flume-ng-dist/target/apache-flume-1.4.0-SNAPSHOT-bin.tar.gz .

  $ tar -zxvf apache-flume-1.4.0-SNAPSHOT-bin.tar.gz
```
2. To run using flume.conf file located in config directory

```
./apache-flume-1.4.0-SNAPSHOT-bin/bin/flume-ng agent -n a1 -c apache-flume-1.4.0-SNAPSHOT-bin/conf/ -f flume.conf
```
