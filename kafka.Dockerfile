FROM amazoncorretto:17 AS kafka-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

## Download spark and hadoop dependencies and install

# ENV variables
ENV KAFKA_VERSION=4.0.0
ENV SCALA_VERSION=2.13

ENV KAFKA_HOME=${KAFKA_HOME:-"/opt/kafka"}

# Download spark
# see resources: https://dlcdn.apache.org/spark/spark-3.5.5/
# filename: spark-3.5.5-bin-hadoop3.tgz
RUN mkdir -p ${SPARK_HOME} \
    && curl https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && tar xvzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz --directory ${KAFKA_HOME} --strip-components 1 \
    && rm -rf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

# Add spark binaries to shell and enable execution
RUN chmod u+x ${KAFKA_HOME}/bin/*
ENV PATH="$PATH:$KAFKA_HOME/bin"

# Add a spark config for all nodes
COPY kafka-config/* "$KAFKA_HOME/config/"


FROM spark-base AS pyspark

# Install python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt


FROM pyspark AS pyspark-runner

## Download iceberg spark runtime
#RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.0/iceberg-spark-runtime-3.5_2.12-1.9.0.jar -Lo /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar

# Download delta jars
RUN curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.1/delta-spark_2.12-3.3.1.jar -Lo /opt/spark/jars/delta-spark_2.12-3.3.1.jar

## Download hudi jars
#RUN curl https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/0.15.0/hudi-spark3-bundle_2.12-0.15.0.jar -Lo /opt/spark/jars/hudi-spark3-bundle_2.12-0.15.0.jar

## Download hive and derby jars
#RUN curl https://repo1.maven.org/maven2/org/apache/derby/derby/10.17.1.0/derby-10.17.1.0.jar -Lo /opt/spark/jars/derby-10.17.1.0.jar \
#    && curl https://repo1.maven.org/maven2/org/apache/hive/hive-exec/4.0.1/hive-exec-4.0.1.jar -Lo /opt/spark/jars/hive-exec-4.0.1.jar \
#    && curl https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/4.0.1/hive-metastore-4.0.1.jar -Lo /opt/spark/jars/hive-metastore-4.0.1.jar

# Download S3 jars
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -Lo /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.782/aws-java-sdk-bundle-1.12.782.jar -Lo /opt/spark/jars/aws-java-sdk-bundle-1.12.782.jar

# Download Structure Streaming Spark jars
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar -Lo /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar \
    && curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.5.5/spark-sql_2.12-3.5.5.jar -Lo /opt/spark/jars/spark-sql_2.12-3.5.5.jar

# Download metadata platform OpenLineage jars
RUN curl https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.32.0/openlineage-spark_2.12-1.32.0.jar -Lo /opt/spark/jars/openlineage-spark_2.12-1.32.0.jar

COPY entrypoint.sh .
RUN chmod u+x /opt/spark/entrypoint.sh


# Optionally install Jupyter
FROM pyspark-runner AS pyspark-jupyter

RUN pip3 install notebook

ENV JUPYTER_PORT=8889

ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --allow-root --ip=0.0.0.0 --port=${JUPYTER_PORT}"
 # --ip=0.0.0.0 - listen all interfaces
 # --port=${JUPYTER_PORT} - listen ip on port 8889
 # --allow-root - to run Jupyter in this container by root user. It is adviced to change the user to non-root.


ENTRYPOINT ["./entrypoint.sh"]
CMD [ "bash" ]

# Now go to interactive shell mode
# -$ docker exec -it spark-master /bin/bash
# then execute
# -$ pyspark

# If Jupyter is installed, you will see an URL: `http://127.0.0.1:8889/?token=...`
# This will open Jupyter web UI in your host machine browser.
# Then go to /warehouse/ and test the installation.
