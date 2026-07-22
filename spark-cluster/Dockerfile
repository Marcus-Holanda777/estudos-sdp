# Dockerfile para o cluster Spark 4.1.3
# Baseado em Ubuntu 22.04 para estabilidade e suporte nativo ao OpenJDK 21

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_VERSION=21
ENV SPARK_VERSION=4.1.3
ENV HADOP_VERSION=3
ENV SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOP_VERSION}.tgz
ENV ICEBERG_VERSION=1.11.0
ENV ICEBERG_JAR_URL=https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    procps \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y openjdk-${JAVA_VERSION}-jdk && rm -rf /var/lib/apt/lists/*

RUN wget ${SPARK_DOWNLOAD_URL} -O /tmp/spark.tgz && \
    mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz && \
    wget ${ICEBERG_JAR_URL} -O /opt/spark/jars/iceberg-spark-runtime.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar -O /opt/spark/jars/spark-sql-kafka-0-10.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar -O /opt/spark/jars/spark-token-provider-kafka-0-10.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar -O /opt/spark/jars/kafka-clients.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar -O /opt/spark/jars/commons-pool2.jar



RUN pip3 install pyspark[pipelines]==${SPARK_VERSION}

COPY spark-iceberg.conf /opt/spark/conf/spark-defaults.conf

COPY scripts /opt/spark/scripts
RUN chmod +x /opt/spark/scripts/*.sh

WORKDIR /opt/spark
