#!/bin/bash
hadoop fs -mkdir /user/hadoop/ITBA/TP1/INPUT/SAMPLE/

hadoop fs -copyFromLocal data/hbase/itba_tp1_airports /user/hadoop/itba_tp1_airports
hadoop fs -copyFromLocal data/hbase/itba_tp1_planes /user/hadoop/itba_tp1_planes
hbase org.apache.hadoop.hbase.mapreduce.Import itba_tp1_airports itba_tp1_airports
hbase org.apache.hadoop.hbase.mapreduce.Import itba_tp1_planes itba_tp1_planes

