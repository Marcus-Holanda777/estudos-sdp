import pyspark.pipelines as dp
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

# No Spark 4.1 SDP, a sessão 'spark' é injetada automaticamente no namespace do módulo.
spark = SparkSession.getActiveSession()


@dp.materialized_view(name="vendas_raw")
def load_vendas() -> DataFrame:
    """
    Lê os dados brutos de vendas de um arquivo CSV.
    """
    return spark.read.csv("/tmp/vendas.csv", header=True, inferSchema=True)

@dp.materialized_view(name="vendas_alta_performance")
def high_value_sales() -> DataFrame:
    """
    Cria uma view materializada filtrando apenas vendas com valor superior a 100.
    Esta view depende de 'vendas_raw'.
    """
    df = spark.table("vendas_raw")
    return df.filter(F.col("valor") > 100)

@dp.materialized_view(name="resumo_vendas")
def sales_summary() -> DataFrame:
    """
    Agrega o total de vendas por produto.
    """
    df = spark.table("vendas_alta_performance")
    return df.groupBy("produto").agg(F.sum("valor").alias("total_valor"))
