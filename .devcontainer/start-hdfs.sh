#!/bin/bash

# Check if the NameNode is already formatted by looking for the VERSION file
if [ ! -f /opt/hadoop/data/namenode/current/VERSION ]; then
  echo "Formatting NameNode..."
  $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
else
  echo "NameNode already formatted."
fi

# Export user environment variables
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

# Start the HDFS services
$HADOOP_HOME/bin/hdfs --daemon start namenode
$HADOOP_HOME/bin/hdfs --daemon start datanode
$HADOOP_HOME/bin/hdfs --daemon start secondarynamenode

# Keep the container running
tail -f /dev/null
