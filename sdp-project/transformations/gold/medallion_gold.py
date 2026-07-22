import pyspark.pipelines as dp
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

# Sessão Spark ativa injetada pelo framework SDP
spark = SparkSession.getActiveSession()

# ==============================================================================
# 1. CAMADA SILVER UNIFICADA
# ==============================================================================
@dp.materialized_view(name="silver.vendas_unificadas")
def combine_silver_vendas() -> DataFrame:
    """
    Camada Silver Unificada: Consolida os datasets 'silver_vendas_batch' e 'silver_vendas_kafka'
    e desduplica por id_venda.
    """
    df_batch = spark.table("local.silver.vendas_batch")
    df_kafka = spark.table("local.silver.vendas_kafka")
    
    df_combined = df_batch.unionByName(df_kafka)
    return df_combined.dropDuplicates(["id_venda"])


# ==============================================================================
# 2. CAMADA GOLD (DATA MARTS / AGGREGATIONS)
# ==============================================================================
@dp.materialized_view(name="gold.resumo_diario_vendas")
def create_gold_resumo_diario() -> DataFrame:
    """
    Camada Gold: Datamart de Vendas Diárias por Categoria consumindo de 'silver_vendas_unificadas'.
    """
    df_silver = spark.table("local.silver.vendas_unificadas")
    
    return (
        df_silver
        .withColumn("dt_venda", F.to_date(F.col("data_venda")))
        .groupBy("dt_venda", "categoria")
        .agg(
            F.countDistinct("id_venda").alias("qtd_pedidos_unicos"),
            F.sum("quantidade").alias("total_itens_vendidos"),
            F.round(F.sum("valor_total_item"), 2).alias("receita_total"),
            F.round(F.avg("valor_total_item"), 2).alias("ticket_medio_item")
        )
        .orderBy(F.col("dt_venda").desc(), F.col("receita_total").desc())
    )


@dp.materialized_view(name="gold.desempenho_canais")
def create_gold_desempenho_canais() -> DataFrame:
    """
    Camada Gold: Datamart de Desempenho por Canal de Venda consumindo de 'silver_vendas_unificadas'.
    """
    df_silver = spark.table("local.silver.vendas_unificadas")
    
    return (
        df_silver
        .groupBy("canal_venda", "categoria")
        .agg(
            F.countDistinct("id_venda").alias("total_pedidos"),
            F.round(F.sum("valor_total_item"), 2).alias("faturamento_total"),
            F.round(F.avg("valor_unitario"), 2).alias("preco_medio_produto")
        )
        .orderBy(F.col("faturamento_total").desc())
    )
