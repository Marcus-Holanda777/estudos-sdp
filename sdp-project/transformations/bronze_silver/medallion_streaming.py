import pyspark.pipelines as dp
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Sessão Spark ativa injetada pelo framework SDP
spark = SparkSession.getActiveSession()

# Schema padronizado para o payload JSON do evento de venda no Kafka
kafka_venda_payload_schema = StructType([
    StructField("id_venda", StringType(), True),
    StructField("data_venda", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("canal_venda", StringType(), True)
])

# ==============================================================================
# 1. CAMADA BRONZE (RAW / INGESTION) - STREAMING KAFKA
# ==============================================================================
@dp.table(name="bronze.vendas_kafka")
def ingest_bronze_vendas_kafka() -> DataFrame:
    """
    Camada Bronze Streaming: Lê mensagens do Kafka e cria a Streaming Table 'bronze_vendas_kafka'.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "vendas_medallion_stream")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr(
            "CAST(value AS STRING) as raw_payload",
            "topic as _kafka_topic",
            "partition as _kafka_partition",
            "offset as _kafka_offset",
            "timestamp as _kafka_timestamp"
        )
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_source", F.lit("STREAMING_KAFKA"))
    )


# ==============================================================================
# 2. CAMADA SILVER (CLEANED & PARSED) - STREAMING KAFKA
# ==============================================================================
@dp.materialized_view(name="silver.vendas_kafka")
def process_silver_vendas_kafka() -> DataFrame:
    """
    Camada Silver Streaming: Parseia o JSON bruto consumindo do dataset 'bronze_vendas_kafka'.
    """
    df_bronze = spark.table("local.bronze.vendas_kafka")
    
    df_parsed = df_bronze.withColumn(
        "payload", F.from_json(F.col("raw_payload"), kafka_venda_payload_schema)
    ).select("payload.*", "_ingestion_time", "_source")
    
    return (
        df_parsed
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
