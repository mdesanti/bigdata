#!/bin/bash

source set_environment.sh
hadoop namenode -format
start-all.sh
hadoop fs -ls /
