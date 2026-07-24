import pyspark.pipelines as dp
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

spark = SparkSession.active()

ICEBERG_PROPERTIES = {
    "write.delete.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
}


@dp.materialized_view(
    name="dbo.silver_vendas_unificadas", table_properties=ICEBERG_PROPERTIES
)
def silver_vendas_unificadas() -> DataFrame:
    """Consolida os dados sanitizados de 'silver_vendas_batch' e 'silver_vendas_kafka', desduplicando vendas por id_venda."""
    df_batch = spark.table("local.dbo.silver_vendas_batch")
    df_kafka = spark.table("local.dbo.silver_vendas_kafka")

    df_combined = df_batch.unionByName(df_kafka)
    return df_combined.dropDuplicates(["id_venda"])


@dp.materialized_view(
    name="dbo.gold_resumo_diario_vendas", table_properties=ICEBERG_PROPERTIES
)
def gold_resumo_diario_vendas() -> DataFrame:
    """Gera o Datamart Gold de Vendas Diarias por Categoria com agregacoes de receita, quantidade e ticket medio."""
    df_silver = spark.table("local.dbo.silver_vendas_unificadas")

    return (
        df_silver.withColumn("dt_venda", F.to_date(F.col("data_venda")))
        .groupBy("dt_venda", "categoria")
        .agg(
            F.countDistinct("id_venda").alias("qtd_pedidos_unicos"),
            F.sum("quantidade").alias("total_itens_vendidos"),
            F.round(F.sum("valor_total_item"), 2).alias("receita_total"),
            F.round(F.avg("valor_total_item"), 2).alias("ticket_medio_item"),
        )
        .orderBy(F.col("dt_venda").desc(), F.col("receita_total").desc())
    )


@dp.materialized_view(
    name="dbo.gold_desempenho_canais", table_properties=ICEBERG_PROPERTIES
)
def gold_desempenho_canais() -> DataFrame:
    """Gera o Datamart Gold de Desempenho por Canal de Venda e Categoria com metricas consolidadas de faturamento."""
    df_silver = spark.table("local.dbo.silver_vendas_unificadas")

    return (
        df_silver.groupBy("canal_venda", "categoria")
        .agg(
            F.countDistinct("id_venda").alias("total_pedidos"),
            F.round(F.sum("valor_total_item"), 2).alias("faturamento_total"),
            F.round(F.avg("valor_unitario"), 2).alias("preco_medio_produto"),
        )
        .orderBy(F.col("faturamento_total").desc())
    )
