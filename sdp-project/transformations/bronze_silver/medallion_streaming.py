import pyspark.pipelines as dp
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.getActiveSession()

kafka_venda_payload_schema = StructType(
    [
        StructField("id_venda", StringType(), True),
        StructField("data_venda", StringType(), True),
        StructField("cliente_id", StringType(), True),
        StructField("produto", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("valor", DoubleType(), True),
        StructField("quantidade", IntegerType(), True),
        StructField("canal_venda", StringType(), True),
    ]
)


@dp.table(
    table_properties={
        "write.delete.mode": "merge-on-read",
        "write.merge.mode": "merge-on-read",
        "write.update.mode": "merge-on-read",
    }
)
def bronze_vendas_kafka() -> DataFrame:
    """Le continuamente as mensagens do topico Kafka 'vendas_medallion_stream' e as grava na Streaming Table 'bronze_vendas_kafka'."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "vendas_medallion_stream")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr(
            "CAST(value AS STRING) as raw_payload",
            "topic as _kafka_topic",
            "partition as _kafka_partition",
            "offset as _kafka_offset",
            "timestamp as _kafka_timestamp",
        )
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_source", F.lit("STREAMING_KAFKA"))
    )


@dp.materialized_view
def silver_vendas_kafka() -> DataFrame:
    """Parseia o payload JSON bruto da tabela Bronze Streaming do Kafka, aplicando limpeza e tipagem para a camada Silver."""
    df_bronze = spark.table("bronze_vendas_kafka")

    df_parsed = df_bronze.withColumn(
        "payload", F.from_json(F.col("raw_payload"), kafka_venda_payload_schema)
    ).select("payload.*", "_ingestion_time", "_source")

    return (
        df_parsed.filter(F.col("id_venda").isNotNull() & (F.col("valor") > 0))
        .withColumn("id_venda", F.trim(F.col("id_venda")))
        .withColumn("data_venda", F.to_timestamp(F.col("data_venda")))
        .withColumn("cliente_id", F.trim(F.col("cliente_id")))
        .withColumn("produto", F.trim(F.col("produto")))
        .withColumn("categoria", F.upper(F.trim(F.col("categoria"))))
        .withColumn("canal_venda", F.upper(F.trim(F.col("canal_venda"))))
        .withColumn("valor_unitario", F.col("valor").cast(DoubleType()))
        .withColumn("quantidade", F.col("quantidade").cast(IntegerType()))
        .withColumn(
            "valor_total_item",
            F.round(F.col("valor_unitario") * F.col("quantidade"), 2),
        )
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
            "_source",
        )
    )
