#!/bin/bash

# ==============================================================================
# 0. Processamento de Parâmetros e Flags CLI
# ==============================================================================
SIMULATE=false
CLEAN=false
CLEAN_ONLY=false
SKIP_DOCKER=false

for arg in "$@"; do
  case $arg in
    --simulate|-s)
      SIMULATE=true
      ;;
    --clean|-c)
      CLEAN=true
      ;;
    --clean-only)
      CLEAN=true
      CLEAN_ONLY=true
      ;;
    --skip-docker|--no-docker|--pipeline-only|-p)
      SKIP_DOCKER=true
      ;;
    --help|-h)
      echo "📖 Uso do script run-sdp.sh:"
      echo "  ./run-sdp.sh                          : Execução padrão (sobe docker compose + roda pipelines)"
      echo "  ./run-sdp.sh --skip-docker | -p       : Roda apenas os pipelines sem executar o docker compose"
      echo "  ./run-sdp.sh --simulate | -s          : Ativa o simulador de streaming Kafka durante o pipeline"
      echo "  ./run-sdp.sh --clean | -c             : Limpa todos os dados, checkpoints e armazém Iceberg antes de rodar"
      echo "  ./run-sdp.sh --clean-only            : Limpa todos os dados, checkpoints e para o ambiente (sem rodar pipelines)"
      echo "  ./run-sdp.sh -p -s                    : Roda apenas os pipelines e simulação sem mexer no Docker"
      echo "  ./run-sdp.sh -c -s                    : Limpa tudo e roda com simulador ativado"
      exit 0
      ;;
  esac
done

# ==============================================================================
# 1. Limpeza de Dados e Checkpoints (se a flag --clean / -c for utilizada)
# ==============================================================================
if [ "$CLEAN" = true ]; then
  echo "🧹 [CLEAN] Derrubando containers Docker e limpando dados (checkpoints, warehouse Iceberg e metastore)..."
  docker compose -f spark-cluster/docker-compose.yml down 2>/dev/null || true
  rm -rf iceberg-warehouse/*
  rm -rf spark-cluster/spark-logs/*
  rm -rf sdp-project/metastore_db sdp-project/derby.log
  rm -rf sdp-project/checkpoints/*
  echo "✨ Limpeza concluída com sucesso!"

  if [ "$CLEAN_ONLY" = true ]; then
    echo "👋 Saindo (--clean-only finalizado)."
    exit 0
  fi
fi

# ==============================================================================
# 2. Inicialização do Cluster Distribuído e Broker Kafka
# ==============================================================================
if [ "$SKIP_DOCKER" = true ]; then
  echo "⏩ [SKIP DOCKER] Pulando 'docker compose up' (assumindo cluster Docker já ativo e funcional)..."
elif [ "$CLEAN" = true ]; then
  echo "🚀 [CLEAN] Recriando cluster Spark, JupyterLab e Kafka do zero..."
  docker compose -f spark-cluster/docker-compose.yml up -d --build --force-recreate --remove-orphans
  docker exec spark-connect rm -rf /opt/spark/sdp-project/checkpoints/* 2>/dev/null || true
else
  echo "🚀 Mantendo cluster Spark, JupyterLab e Kafka existentes (preservando tópicos e offsets)..."
  docker compose -f spark-cluster/docker-compose.yml up -d
fi

# ==============================================================================
# 3. Criação do Schema 'dbo' no Catálogo Apache Iceberg
# ==============================================================================
echo "📦 Criando Schema 'dbo' no Apache Iceberg..."
docker exec spark-master spark-sql --master spark://spark-master:7077 -e "
  CREATE SCHEMA IF NOT EXISTS local.dbo;
  SHOW SCHEMAS IN local;
"

# ==============================================================================
# 4. Envio de Mensagens para o Tópico Kafka
# ==============================================================================
sleep 2 # Aguarda validação do ambiente

if [ "$SIMULATE" = true ]; then
  echo "📡 Modo Simulação Ativado! Iniciando o gerador contínuo em Bash (stream_simulator.sh) em segundo plano..."
  ./sdp-project/stream_simulator.sh &
  SIMULATOR_PID=$!
  echo "PID do Simulador: $SIMULATOR_PID"
  sleep 20 # Permite gerar alguns eventos iniciais antes de rodar os pipelines
else
  ID1=$(( 2000 + (RANDOM % 3001) ))
  ID2=$(( 2000 + (RANDOM % 3001) ))
  ID3=$(( 2000 + (RANDOM % 3001) ))
  echo "✉️ Enviando eventos de teste estáticos com IDs aleatórios (VND-$ID1, VND-$ID2, VND-$ID3)..."
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic vendas_medallion_stream <<EOF
{"id_venda": "VND-$ID1", "data_venda": "2026-07-22 15:10:00", "cliente_id": "CLI-010", "produto": "Webcam Full HD", "categoria": "Perifericos", "valor": 350.00, "quantidade": 1, "canal_venda": "E-commerce"}
{"id_venda": "VND-$ID2", "data_venda": "2026-07-22 15:20:00", "cliente_id": "CLI-011", "produto": "Suporte Articulado", "categoria": "Acessorios", "valor": 120.00, "quantidade": 2, "canal_venda": "E-commerce"}
{"id_venda": "VND-$ID3", "data_venda": "2026-07-22 15:35:00", "cliente_id": "CLI-012", "produto": "Teclado Mecanico", "categoria": "Perifericos", "valor": 250.00, "quantidade": 1, "canal_venda": "Loja_Fisica"}
EOF
fi

# ==============================================================================
# 5. EXECUÇÃO PIPELINE 1: BRONZE & SILVER BASE (Dry-Run + Run)
# ==============================================================================
echo "----------------------------------------------------------------------"
echo "🔍 1.1 [DRY-RUN] Validando DAG - Pipeline Bronze/Silver Base"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines dry-run --spec spark-pipeline-bronze-silver.yml"

echo "----------------------------------------------------------------------"
echo "⚙️ 1.2 [RUN] Executando Pipeline Bronze/Silver Base"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines run --spec spark-pipeline-bronze-silver.yml"

# ==============================================================================
# 6. EXECUÇÃO PIPELINE 2: SILVER UNIFICADA & GOLD (Dry-Run + Run)
# ==============================================================================
echo "----------------------------------------------------------------------"
echo "🔍 2.1 [DRY-RUN] Validando DAG - Pipeline Silver Unificada & Gold"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines dry-run --spec spark-pipeline-gold.yml"

echo "----------------------------------------------------------------------"
echo "⚙️ 2.2 [RUN] Executando Pipeline Silver Unificada & Gold"
echo "----------------------------------------------------------------------"
docker exec spark-connect bash -c "cd /opt/spark/sdp-project && spark-pipelines run --spec spark-pipeline-gold.yml"

# ==============================================================================
# 7. VALIDAÇÃO SQL & CONSULTA DAS TABELAS GERADAS NO SCHEMA dbo
# ==============================================================================
echo "----------------------------------------------------------------------"
echo "📦 SCHEMA dbo - Tabelas Medallion (bronze_*, silver_*, gold_*)"
echo "----------------------------------------------------------------------"
docker exec spark-master bash -c "spark-sql \
  --master spark://spark-master:7077 \
  -e 'SHOW TABLES IN dbo;'"

echo "----------------------------------------------------------------------"
echo "🥉 CAMADA BRONZE (dbo.bronze_vendas_batch)"
echo "----------------------------------------------------------------------"
docker exec spark-master bash -c "spark-sql \
  --master spark://spark-master:7077 \
  -e 'SELECT id_venda, produto, valor, _source, _ingestion_time FROM local.dbo.bronze_vendas_batch;'"

echo "----------------------------------------------------------------------"
echo "🥈 CAMADA SILVER UNIFICADA (dbo.silver_vendas_unificadas)"
echo "----------------------------------------------------------------------"
docker exec spark-master bash -c "spark-sql \
  --master spark://spark-master:7077 \
  -e 'SELECT id_venda, data_venda, produto, categoria, valor_total_item, canal_venda, _source FROM local.dbo.silver_vendas_unificadas;'"

echo "----------------------------------------------------------------------"
echo "🥇 CAMADA GOLD (dbo.gold_resumo_diario_vendas & dbo.gold_desempenho_canais)"
echo "----------------------------------------------------------------------"
docker exec spark-master bash -c "spark-sql \
  --master spark://spark-master:7077 \
  -e 'SELECT * FROM local.dbo.gold_resumo_diario_vendas; SELECT * FROM local.dbo.gold_desempenho_canais;'"
echo "----------------------------------------------------------------------"

if [ -n "$SIMULATOR_PID" ]; then
  kill $SIMULATOR_PID 2>/dev/null || true
  echo "🛑 Simulador em segundo plano finalizado."
fi

echo "✅ Execução sequencial dos 2 Pipelines concluída com sucesso!"
echo "----------------------------------------------------------------------"
echo "📓 JUPYTERLAB INTERATIVO DISPONÍVEL!"
echo "   Acesse: http://localhost:8888"
echo "   Notebook de exemplo: notebooks/playground_medallion.ipynb"
echo "----------------------------------------------------------------------"
