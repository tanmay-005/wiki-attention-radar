# One image for every Hadoop node + the edge (client) node.
# Ubuntu 22.04 + Java 11 + Hadoop 3.3.6 + Spark 3.5.3 + Python 3.
FROM eclipse-temurin:11-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.3

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip curl wget procps netcat-openbsd less vim-tiny unzip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Hadoop: copied from the official Apache image on Docker Hub (fast CDN)
COPY --from=apache/hadoop:3.3.6 /opt/hadoop /opt/hadoop
RUN rm -rf /opt/hadoop/share/doc

# Spark: the pyspark pip package ships the full Spark runtime (fast PyPI CDN)
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION} \
    && ln -s "$(python3 -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" /opt/spark \
    && chmod +x /opt/spark/bin/*

# Python libraries used by scripts, MapReduce tasks and Spark jobs
COPY requirements-edge.txt /tmp/requirements-edge.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-edge.txt

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:$PATH

RUN mkdir -p /data/hdfs/namenode /data/hdfs/datanode /data/raw-cache

COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["edge"]
