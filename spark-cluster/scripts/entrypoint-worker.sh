#!/bin/bash
# Inicia o Spark Worker seguindo o padrão do lakehouse-at-home
# Usa o script de inicialização oficial e monitora os logs para manter o container ativo

MASTER_URL=${1:-"spark://spark-master:7077"}
WEBUI_PORT=${2:-"8081"}

echo "Iniciando Spark Worker conectando ao Master: $MASTER_URL na porta $WEBUI_PORT..."
/opt/spark/sbin/start-worker.sh $MASTER_URL --webui-port $WEBUI_PORT

# Aguarda um momento para os logs serem criados
sleep 2

echo "Monitorando logs do Spark Worker..."
tail -f /opt/spark/logs/*.out
