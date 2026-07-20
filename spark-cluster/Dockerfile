# Dockerfile para o cluster Spark 4.1.0
# Baseado em Ubuntu 22.04 para estabilidade e suporte nativo ao OpenJDK 17

FROM ubuntu:22.04

# Evitar prompts interativos durante a instalação
ENV DEBIAN_FRONTEND=noninteractive

# 1. Atualização do sistema e instalação de dependências básicas
# wget e curl: necessários para baixar os binários do Spark
# procps: essencial para que o Spark consiga gerenciar processos no Linux
# python3-pip: necessário para o PySpark e gestão de dependências do SDP
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    procps \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 2. Instalação do Java 17
# O Spark 4.x exige Java 17 ou superior para aproveitar melhoras de performance e segurança da JVM
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

# 3. Instalação Manual do Apache Spark 4.1.0
# Baixamos a versão bin-hadoop3 (padrão para a maioria dos ambientes Hadoop)
# Usamos o CDN oficial da Apache para maior velocidade de download
ENV SPARK_VERSION=4.1.3
ENV HADOP_VERSION=3
ENV SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOP_VERSION}.tgz
# Iceberg Runtime para Spark 4.0 (compatível com 4.x) - Scala 2.13
ENV ICEBERG_VERSION=1.11.0
ENV ICEBERG_JAR_URL=https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar

RUN wget ${SPARK_DOWNLOAD_URL} -O /tmp/spark.tgz && \
    mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz && \
    wget ${ICEBERG_JAR_URL} -O /opt/spark/jars/iceberg-spark-runtime.jar

# Instalação das dependências de Pipelines do PySpark
# O extra [pipelines] instala pyyaml, pandas e pyarrow, essenciais para o SDP
RUN pip3 install pyspark[pipelines]==${SPARK_VERSION}

# 4. Configuração de Variáveis de Ambiente
# JAVA_HOME e SPARK_HOME são fundamentais para que o Spark e o Hadoop localizem seus binários e bibliotecas
# PYSPARK_PYTHON define qual interpretador python o Spark deve usar nos Workers e Driver
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Configuração do Catálogo Iceberg
COPY spark-iceberg.conf /opt/spark/conf/spark-defaults.conf


# 5. Configuração de permissões e scripts de entrada
# Copiamos os scripts para dentro da imagem e garantimos que sejam executáveis
COPY scripts /opt/spark/scripts
RUN chmod +x /opt/spark/scripts/*.sh

# Definimos o diretório de trabalho
WORKDIR /opt/spark

# O comando de inicialização será definido no docker-compose.yml
