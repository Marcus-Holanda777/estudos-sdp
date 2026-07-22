#!/bin/bash
# Inicia o Spark Connect Server (Driver)
# Este container atua como a ponte entre o cliente (CLI/Notebook) e o Cluster Spark

echo "Iniciando Spark Connect Server..."
# Iniciamos o connect server. O --master aponta para o container do Master.
/opt/spark/sbin/start-connect-server.sh --master spark://spark-master:7077 --total-executor-cores 1 --executor-cores 1 --executor-memory 2g --conf spark.ui.port=4050

# Aguarda um momento para os logs serem criados
sleep 2

echo "Monitorando logs do Spark Connect Server..."
tail -f /opt/spark/logs/*.out
