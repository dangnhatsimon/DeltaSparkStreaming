FROM bitnami/spark:latest

# Set the working directory
WORKDIR /opt/bitnami/spark

# Copy the Python requirements file
COPY requirements.txt ./requirements.txt

# Switch to root user for installations
USER root

# Install Python and dependencies
RUN apt-get update --fix-missing && \
    apt-get install -y curl python3-pip && \
    pip3 install --no-cache-dir -r ./requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.31.16/s3-2.31.16.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.782/aws-java-sdk-1.12.782.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/3.3.1/delta-spark_2.13-3.3.1.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.1/delta-storage-3.3.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.3/spark-sql-kafka-0-10_2.13-3.5.3.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.13/3.5.3/spark-sql_2.13-3.5.3.jar \
    && mv s3-2.31.16.jar /jars \
    && mv aws-java-sdk-1.12.782.jar /jars \
    && mv delta-spark_2.13-3.3.1.jar /jars \
    && mv delta-storage-3.3.1.jar /jars \
    && mv spark-sql-kafka-0-10_2.13-3.5.3.jar /jars \
    && mv spark-sql_2.13-3.5.3.jar /jars
# Since the cluster will deserialize your app and run it, the cluster need similar depenecies.
# ie. if your app uses numpy
#RUN pip install numpy