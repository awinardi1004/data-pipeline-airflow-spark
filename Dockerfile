FROM apache/airflow:2.8.1

USER root

# Install dependencies
RUN apt-get update && apt-get install -y curl tar

# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    rm -rf /opt/spark && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow