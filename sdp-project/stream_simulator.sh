#!/bin/bash

# Simulador simples em Bash para alimentar o tópico Kafka continuamente
TOPIC="vendas_medallion_stream"
CONTAINER_NAME="kafka"
DELAY=2 # segundos entre eventos

PRODUTOS=("Webcam Full HD" "Teclado Mecanico" "Mouse Sem Fio" "Monitor 27 IPS" "Suporte Articulado")
CATEGORIAS=("Perifericos" "Perifericos" "Perifericos" "Monitores" "Acessorios")
VALORES=(350 280 150 1400 120)
CANAIS=("E-commerce" "Loja_Fisica" "App_Mobile" "Marketplace")

echo "🚀 Iniciando Simulador de Eventos em Bash..."
echo "📌 Tópico: $TOPIC | Pressione CTRL+C para cancelar."
echo ""

while true; do
  # Gera um número aleatório de ID de venda no intervalo de 2000 a 5000
  SEQ=$(( 2000 + (RANDOM % 3001) ))
  IDX=$((RANDOM % 5))
  CANAL_IDX=$((RANDOM % 4))
  QTD=$(( (RANDOM % 3) + 1 ))
  CLIENTE_ID="CLI-$(( (RANDOM % 900) + 100 ))"
  DATA_ATUAL=$(date +"%Y-%m-%d %H:%M:%S")

  PROD="${PRODUTOS[$IDX]}"
  CAT="${CATEGORIAS[$IDX]}"
  VAL="${VALORES[$IDX]}"
  CANAL="${CANAIS[$CANAL_IDX]}"

  JSON_PAYLOAD=$(cat <<EOF
{"id_venda": "VND-$SEQ", "data_venda": "$DATA_ATUAL", "cliente_id": "$CLIENTE_ID", "produto": "$PROD", "categoria": "$CAT", "valor": $VAL.00, "quantidade": $QTD, "canal_venda": "$CANAL"}
EOF
)

  echo "$JSON_PAYLOAD" | docker exec -i $CONTAINER_NAME /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic $TOPIC > /dev/null 2>&1

  echo "✉️  [EVENTO ENVIADO] VND-$SEQ - $PROD ($CANAL) às $DATA_ATUAL"
  sleep $DELAY
done
