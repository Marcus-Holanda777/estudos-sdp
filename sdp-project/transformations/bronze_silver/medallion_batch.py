import pyspark.pipelines as dp
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Sessão Spark ativa injetada pelo framework SDP
spark = SparkSession.getActiveSession()

# ==============================================================================
# 1. CAMADA BRONZE (RAW / INGESTION) - BATCH
# ==============================================================================
@dp.materialized_view(name="bronze.vendas_batch")
def ingest_bronze_vendas_batch() -> DataFrame:
    """
    Camada Bronze Batch: Ingestão bruta de vendas a partir do CSV.
    """
    df_raw = spark.read.csv(
        "/tmp/data/vendas_medallion.csv",
        header=True,
        inferSchema=True
    )
    return (
        df_raw
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_source", F.lit("BATCH_CSV"))
    )


# ==============================================================================
# 2. CAMADA SILVER (CLEANED & ENRICHED) - BATCH
# ==============================================================================
@dp.materialized_view(name="silver.vendas_batch")
def process_silver_vendas_batch() -> DataFrame:
    """
    Camada Silver Batch: Limpeza, validação de qualidade, casting e colunas derivadas
    consumindo do dataset 'bronze_vendas_batch'.
    """
    df_bronze = spark.table("local.bronze.vendas_batch")
    
    return (
        df_bronze
        .filter(F.col("id_venda").isNotNull() & (F.col("valor") > 0))
        .withColumn("id_venda", F.trim(F.col("id_venda")))
        .withColumn("data_venda", F.to_timestamp(F.col("data_venda")))
        .withColumn("cliente_id", F.trim(F.col("cliente_id")))
        .withColumn("produto", F.trim(F.col("produto")))
        .withColumn("categoria", F.upper(F.trim(F.col("categoria"))))
        .withColumn("canal_venda", F.upper(F.trim(F.col("canal_venda"))))
        .withColumn("valor_unitario", F.col("valor").cast(DoubleType()))
        .withColumn("quantidade", F.col("quantidade").cast(IntegerType()))
        .withColumn("valor_total_item", F.round(F.col("valor_unitario") * F.col("quantidade"), 2))
        .select(
            "id_venda",
            "data_venda",
            "cliente_id",
            "produto",
            "categoria",
            "valor_unitario",
            "quantidade",
            "valor_total_item",
            "canal_venda",
            "_ingestion_time",
            "_source"
        )
    )
