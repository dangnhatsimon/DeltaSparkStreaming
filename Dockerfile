FROM bitnami/spark:latest

# Set the working directory
WORKDIR /opt/bitnami/spark

# Optional: custom logging
COPY log4j2.properties /opt/bitnami/spark/conf/log4j2.properties

# Copy the Python requirements file
COPY requirements.txt ./requirements.txt

# Switch to root user for installations
USER root

# Install Python and dependencies
RUN apt-get update --fix-missing && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir -r ./requirements.txt && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Since the cluster will deserialize your app and run it, the cluster need similar depenecies.
# ie. if your app uses numpy
#RUN pip install numpy