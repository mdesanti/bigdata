#!/bin/bash

mkdir -p data/hbase
bash -c "scp -r hadoop@hadoop-2013-datanode-2:/home/hadoop/crpereyr/hbase_export/* data/hbase"
