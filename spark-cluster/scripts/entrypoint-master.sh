#!/bin/bash
# Inicia o Spark Master seguindo o padrão do lakehouse-at-home
# Usa o script de inicialização oficial e monitora os logs para manter o container ativo

echo "Iniciando Spark Master..."
/opt/spark/sbin/start-master.sh --host spark-master --port 7077 --webui-port 8080

# Aguarda um momento para os logs serem criados
sleep 2

echo "Monitorando logs do Spark Master..."
tail -f /opt/spark/logs/*.out
