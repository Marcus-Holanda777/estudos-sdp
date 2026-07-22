#!/bin/bash

# 1. Sobe a infraestrutura do cluster distribuído
echo "🚀 Subindo cluster Spark e Kafka..."
docker compose -f spark-cluster/docker-compose.yml up -d --build --force-recreate --remove-orphans

# 2. Criar os SCHEMAS / NAMESPACES Medallion (bronze, silver, gold) no catálogo Apache Iceberg
echo "📦 Criando Schemas (bronze, silver, gold) no Apache Iceberg..."
docker exec spark-master spark-sql --master spark://spark-master:7077 -e "
  CREATE SCHEMA IF NOT EXISTS local.bronze;
  CREATE SCHEMA IF NOT EXISTS local.silver;
  CREATE SCHEMA IF NOT EXISTS local.gold;
  SHOW SCHEMAS IN local;
"

# 3. Publicar mensagens JSON de teste no tópico Kafka 'vendas_medallion_stream'
echo "✉️ Enviando eventos de streaming para o tópico Kafka 'vendas_medallion_stream'..."
sleep 5 # Aguarda inicialização do broker Kafka
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic vendas_medallion_stream <<EOF
{"id_venda": "VND-2001", "data_venda": "2026-07-22 15:10:00", "cliente_id": "CLI-010", "produto": "Webcam Full HD", "categoria": "Perifericos", "valor": 350.00, "quantidade": 1, "canal_venda": "E-commerce"}
{"id_venda": "VND-2002", "data_venda": "2026-07-22 15:20:00", "cliente_id": "CLI-011", "produto": "Suporte Articulado", "categoria": "Acessorios", "valor": 120.00, "quantidade": 2, "canal_venda": "E-commerce"}
{"id_venda": "VND-2003", "data_venda": "2026-07-22 15:35:00", "cliente_id": "CLI-012", "produto": "Teclado Mecanico", "categoria": "Perifericos", "valor": 250.00, "quantidade": 1, "canal_venda": "Loja_Fisica"}
EOF

# 4. Executa o pipeline declarativo usando a CLI oficial do Spark Pipelines
echo "⚙️ Executando o Pipeline Declarativo nos Schemas Medallion...[BRONZE-SILVER]"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines run --spec spark-pipeline-bronze-silver.yml"

echo "⚙️ Executando o Pipeline Declarativo nos Schemas Medallion... [GOLD]"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines run --spec spark-pipeline-gold.yml"

# 5. Consulta e Exibição das Tabelas por Schema Medallion
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

echo "✅ Pipeline Medallion executado com sucesso nos Schemas (bronze, silver, gold)!"
