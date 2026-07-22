#!/bin/bash
# Inicia o Spark History Server
# Este servidor lê os logs de eventos gravados em disco e os exibe na UI

echo "Iniciando Spark History Server..."
# O servidor de histórico usa as configurações de spark.history.fs.logDir do spark-defaults.conf
/opt/spark/sbin/start-history-server.sh

# Aguarda um momento para os logs serem criados
sleep 2

echo "Monitorando logs do Spark History Server..."
tail -f /opt/spark/logs/*.out
