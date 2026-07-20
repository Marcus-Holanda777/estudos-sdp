#!/bin/bash

# 1. Sobe o cluster
echo "Subindo cluster Spark..."
docker compose -f spark-cluster/docker-compose.yml up -d --build --force-recreate

# 2. Copia os dados de teste para o Master
echo "Copiando dados de teste..."
docker cp sdp-project/data/vendas.csv spark-master:/tmp/vendas.csv

# 3. Copia o projeto SDP para o Master
echo "Copiando projeto SDP..."
docker cp sdp-project spark-master:/opt/spark/sdp-project

# 4. Criar o schema DBO no catalogo ICEBERG/LOCAL
docker exec -it spark-master spark-sql -e "CREATE NAMESPACE IF NOT EXISTS local.dbo; SHOW NAMESPACES IN local;";


# 5. Executa o pipeline usando a CLI oficial do Spark 4.2
echo "Executando o Pipeline Declarativo..."
docker exec -it spark-master bash -c "cd /opt/spark/sdp-project && spark-pipelines run"

# 6. Exibição dos resultados
echo "--------------------------------------------------"
echo "📊 RESULTADO FINAL DO PIPELINE (Catalogo Iceberg: local.resumo_vendas)"
echo "--------------------------------------------------"
docker exec -it spark-master bash -c "cd /opt/spark/sdp-project && spark-sql \
  -e 'SELECT * FROM dbo.resumo_vendas; SHOW TABLES IN dbo;'"
echo "--------------------------------------------------"

echo "Processamento concluído. Verifique os resultados no Master."
