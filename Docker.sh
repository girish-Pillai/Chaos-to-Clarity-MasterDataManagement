# Use an official Python runtime as the base image
FROM python:3.8-slim-buster

# Set the working directory in the container
WORKDIR /app

# Install necessary packages
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Apache Spark
RUN curl -O https://downloads.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz \
    && tar xzf spark-3.2.0-bin-hadoop3.2.tgz \
    && mv spark-3.2.0-bin-hadoop3.2 /opt/spark \
    && rm spark-3.2.0-bin-hadoop3.2.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install required Python packages
RUN pip install pyspark apache-airflow fuzzywuzzy

# Copy the PySpark code files to the container
COPY function.py /app
COPY Main.py /app
COPY Docker_sparkSubmit_dag.py /app

# Expose necessary ports
EXPOSE 8080

# Set the entrypoint command to start Airflow and run the DAG
CMD ["bash", "-c", "airflow webserver -D && airflow scheduler && airflow dags trigger Docker_sparkSubmit_dag"]


