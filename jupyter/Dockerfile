FROM jupyter/all-spark-notebook:x86_64-latest

ARG SPARK_VERSION=3.5.5

USER root

# Set a custom working directory
WORKDIR /home/jovyan/work

# Copy the Python requirements file
COPY requirements.txt ./requirements.txt

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    make \
    python3 \
    python3-pip \
    python3-venv \
    libxml2 \
    libxml2-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install -r ./requirements.txt

RUN curl -O https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar zxvf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && ln -s /usr/local/spark-${SPARK_VERSION}-bin-hadoop3 /usr/local/spark

RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.31.16/s3-2.31.16.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.782/aws-java-sdk-1.12.782.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/3.3.1/delta-spark_2.13-3.3.1.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.1/delta-storage-3.3.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.3/spark-sql-kafka-0-10_2.13-3.5.3.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.13/3.5.3/spark-sql_2.13-3.5.3.jar \
    && mv s3-2.31.16.jar usr/local/spark/jars \
    && mv aws-java-sdk-1.12.782.jar usr/local/spark/jars \
    && mv delta-spark_2.13-3.3.1.jar usr/local/spark/jars \
    && mv delta-storage-3.3.1.jar usr/local/spark/jars \
    && mv spark-sql-kafka-0-10_2.13-3.5.3.jar usr/local/spark/jars \
    && mv spark-sql_2.13-3.5.3.jar usr/local/spark/jars