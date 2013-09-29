#!/usr/bin

mkdir -p data/dump
bash -c "scp -r hadoop@hadoop-2013-namenode:/home/hadoop/crpereyr/dump/* data/dump"
