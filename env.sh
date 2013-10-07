# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

# Java related environment variables
export JAVA_HOME=/usr/java/jdk1.6.0_25/
export PATH=$PATH:$JAVA_HOME/bin

# Hadoop related environment variables
export HADOOP_INSTALL=/home/hadoop/hadoop-1.0.4
export HADOOP_PATH=/home/hadoop/hadoop-1.0.4
export PIG_INSTALL=/home/hadoop/pig-0.11.1
export HIVE_INSTALL=/home/hadoop/hive-0.9.0
export HBASE_INSTALL=/home/hadoop/hbase-0.94.6.1
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin:$PIG_INSTALL/bin:$HIVE_INSTALL/bin:$HBASE_INSTALL/bin
export PIG_CLASSPATH=$PATH:$HBASE_INSTALL/bin:`$HBASE_INSTALL/bin/hbase classpath`;
