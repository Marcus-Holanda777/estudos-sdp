#!/bin/bash

# ==============================================================================
# 0. Limpeza prévia de dados, checkpoints e logs
# ==============================================================================
echo "🧹 Limpando dados da execução anterior (checkpoints, warehouse Iceberg e metastore)..."
docker compose -f spark-cluster/docker-compose.yml down 2>/dev/null || true
rm -rf iceberg-warehouse/*
rm -rf spark-cluster/spark-logs/*
rm -rf sdp-project/metastore_db sdp-project/derby.log
rm -rf sdp-project/checkpoints/*

# ==============================================================================
# 1. Inicialização do Cluster Distribuído e Broker Kafka
# ==============================================================================
echo "🚀 Subindo cluster Spark e Kafka..."
docker compose -f spark-cluster/docker-compose.yml up -d --build --force-recreate --remove-orphans

# Limpa o diretório de checkpoints montado no volume
docker exec spark-connect rm -rf /opt/spark/sdp-project/checkpoints/* 2>/dev/null || true

# ==============================================================================
# 2. Criação dos Schemas/Namespaces no Catálogo Apache Iceberg
# ==============================================================================
echo "📦 Criando Schemas (bronze, silver, gold) no Apache Iceberg..."
docker exec spark-master spark-sql --master spark://spark-master:7077 -e "
  CREATE SCHEMA IF NOT EXISTS local.bronze;
  CREATE SCHEMA IF NOT EXISTS local.silver;
  CREATE SCHEMA IF NOT EXISTS local.gold;
  SHOW SCHEMAS IN local;
"

# ==============================================================================
# 3. Envio de Mensagens de Teste para o Tópico Kafka
# ==============================================================================
echo "✉️ Enviando eventos de streaming para o tópico Kafka 'vendas_medallion_stream'..."
sleep 5 # Aguarda inicialização do broker Kafka
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic vendas_medallion_stream <<EOF
{"id_venda": "VND-2001", "data_venda": "2026-07-22 15:10:00", "cliente_id": "CLI-010", "produto": "Webcam Full HD", "categoria": "Perifericos", "valor": 350.00, "quantidade": 1, "canal_venda": "E-commerce"}
{"id_venda": "VND-2002", "data_venda": "2026-07-22 15:20:00", "cliente_id": "CLI-011", "produto": "Suporte Articulado", "categoria": "Acessorios", "valor": 120.00, "quantidade": 2, "canal_venda": "E-commerce"}
{"id_venda": "VND-2003", "data_venda": "2026-07-22 15:35:00", "cliente_id": "CLI-012", "produto": "Teclado Mecanico", "categoria": "Perifericos", "valor": 250.00, "quantidade": 1, "canal_venda": "Loja_Fisica"}
EOF

# ==============================================================================
# 4. EXECUÇÃO PIPELINE 1: BRONZE & SILVER (Dry-Run + Run)
# ==============================================================================
echo "----------------------------------------------------------------------"
echo "🔍 1.1 [DRY-RUN] Validando DAG - Pipeline Bronze/Silver"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines dry-run --spec spark-pipeline-bronze-silver.yml"

echo "----------------------------------------------------------------------"
echo "⚙️ 1.2 [RUN] Executando Pipeline Bronze/Silver (Ingestão & Sanitização)"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines run --spec spark-pipeline-bronze-silver.yml"

# ==============================================================================
# 5. EXECUÇÃO PIPELINE 2: GOLD (Dry-Run + Run)
# ==============================================================================
echo "----------------------------------------------------------------------"
echo "🔍 2.1 [DRY-RUN] Validando DAG - Pipeline Gold"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines dry-run --spec spark-pipeline-gold.yml"

echo "----------------------------------------------------------------------"
echo "⚙️ 2.2 [RUN] Executando Pipeline Gold (Datamarts & Business Aggregations)"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines run --spec spark-pipeline-gold.yml"

# ==============================================================================
# 6. VALIDAÇÃO SQL & CONSULTA DAS TABELAS MEDALLION GERADAS
# ==============================================================================
echo "----------------------------------------------------------------------"
echo "🥉 CAMADA BRONZE - Schema: bronze (bronze.vendas_batch & bronze.vendas_kafka)"
echo "----------------------------------------------------------------------"
docker exec spark-master bash -c "spark-sql \
  --master spark://spark-master:7077 \
  -e 'SHOW TABLES IN bronze; SELECT id_venda, produto, valor, _source, _ingestion_time FROM bronze.vendas_batch LIMIT 3;'"

echo "----------------------------------------------------------------------"
echo "🥈 CAMADA SILVER - Schema: silver (silver.vendas_unificadas)"
echo "----------------------------------------------------------------------"
docker exec spark-master bash -c "spark-sql \
  --master spark://spark-master:7077 \
  -e 'SHOW TABLES IN silver; SELECT id_venda, data_venda, produto, categoria, valor_total_item, canal_venda, _source FROM silver.vendas_unificadas;'"

echo "----------------------------------------------------------------------"
echo "🥇 CAMADA GOLD - Schema: gold (gold.resumo_diario_vendas & gold.desempenho_canais)"
echo "----------------------------------------------------------------------"
docker exec spark-master bash -c "spark-sql \
  --master spark://spark-master:7077 \
  -e 'SHOW TABLES IN gold; SELECT * FROM gold.resumo_diario_vendas; SELECT * FROM gold.desempenho_canais;'"
echo "----------------------------------------------------------------------"

echo "✅ Validação dry-run e execução dos Pipelines concluídas com sucesso!"
