import pyspark.pipelines as dp
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.active()

ICEBERG_PROPERTIES = {
    "write.delete.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
}


@dp.materialized_view(
    name="dbo.bronze_vendas_batch", table_properties=ICEBERG_PROPERTIES
)
def bronze_vendas_batch() -> DataFrame:
    """Realiza a ingestao bruta dos dados de vendas do arquivo CSV estatico para a camada Bronze Batch."""
    df_raw = spark.read.csv(
        "/tmp/data/vendas_medallion.csv", header=True, inferSchema=True
    )
    return df_raw.withColumn("_ingestion_time", F.current_timestamp()).withColumn(
        "_source", F.lit("BATCH_CSV")
    )


@dp.materialized_view(
    name="dbo.silver_vendas_batch", table_properties=ICEBERG_PROPERTIES
)
def silver_vendas_batch() -> DataFrame:
    """Aplica higienizacao, filtros de qualidade e calculo de valores derivados para a camada Silver Batch."""
    df_bronze = spark.table("local.dbo.bronze_vendas_batch")

    return (
        df_bronze.filter(F.col("id_venda").isNotNull() & (F.col("valor") > 0))
        .withColumn("id_venda", F.trim(F.col("id_venda")))
        .withColumn("data_venda", F.col("data_venda").cast("timestamp"))
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
