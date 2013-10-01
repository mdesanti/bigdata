#!/bin/bash

source set_environment.sh
hadoop namenode -format
start-all.sh
start-hbase.sh
scripts/load_data.sh
scripts/load_hbase.sh
hadoop fs -ls /
