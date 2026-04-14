# Using a stable Python base image.
FROM python:3.9-slim-bullseye

# --- SYSTEM DEPENDENCIES ---
# Installing Java 11 (required for Spark) and essential tools for downloading/extracting files.
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    tar \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- SPARK INSTALLATION ---
# Downloading and setting up Apache Spark 3.4.1 with Hadoop 3 support.
RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.1-bin-hadoop3.tgz && \
    mv spark-3.4.1-bin-hadoop3 /opt/spark && \
    rm spark-3.4.1-bin-hadoop3.tgz

# --- ENVIRONMENT VARIABLES ---
# Essential paths for the Spark engine and Java Runtime Environment.
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

# Setting the working directory inside the container.
WORKDIR /app

# --- PYTHON DEPENDENCIES ---
# Installing the specific libraries (pyspark, kafka-python, pymongo, etc.) from requirements.txt.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copying the rest of the project source code.
COPY . .