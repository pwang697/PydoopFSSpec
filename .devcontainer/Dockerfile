# Use Ubuntu as the base image
FROM mcr.microsoft.com/devcontainers/base:jammy

# Install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-8-jdk wget curl vim python3 python3-pip python3-dev build-essential libtirpc-dev && \
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

# Create a symbolic link to the rpc directory
RUN ln -s /usr/include/tirpc/rpc /usr/include/rpc

# Set CFLAGS and CXXFLAGS to include the tirpc headers
ENV CFLAGS="-I/usr/include/tirpc"
ENV CXXFLAGS="-I/usr/include/tirpc"

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.2.4/hadoop-3.2.4.tar.gz && \
    tar -xzvf hadoop-3.2.4.tar.gz && \
    mv hadoop-3.2.4 /opt/hadoop && \
    rm hadoop-3.2.4.tar.gz

# Copy Hadoop configuration files
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# Create Hadoop data directories
RUN mkdir -p /opt/hadoop/data/namenode /opt/hadoop/data/datanode

# Configure Hadoop environment variables
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_LOG_DIR=/opt/hadoop/data/logs" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

# Create Hadoop log directory
RUN mkdir -p /opt/hadoop/data/logs

# Create a workspace directory
WORKDIR /workspace

# Copy the requirements.txt file to the container
COPY requirements.txt /workspace/requirements.txt

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Set the default shell to bash
SHELL ["/bin/bash", "-c"]

# Expose HDFS ports
EXPOSE 9870 8020

# Set HDFS user environment variables
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root

# Start HDFS
COPY start-hdfs.sh /opt/start-hdfs.sh
RUN chmod +x /opt/start-hdfs.sh
CMD ["/opt/start-hdfs.sh"]
