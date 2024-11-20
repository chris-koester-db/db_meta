from databricks.connect.session import DatabricksSession
from pyspark.sql import SparkSession, DataFrame
import dlt

# Get SparkSession
# https://docs.databricks.com/dev-tools/databricks-connect.html
def get_spark() -> SparkSession:
    try:
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate() # type: ignore

def read_delta_dlt(spark, task):    
    @dlt.expect_all_or_drop(task['expectations'])
    @dlt.table(name=f"{task['tgt_table']}", comment=f"{task['tgt_table']}")
    def _read_delta_dlt() -> DataFrame:
        return spark.read.format("delta").table(
            f"{task['src_catalog']}.{task['src_schema']}.{task['src_table']}"
        )

def stream_delta_dlt(spark, task):    
    @dlt.expect_all_or_drop(task['expectations'])
    @dlt.table(name=f"{task['tgt_table']}", comment=f"{task['tgt_table']}")
    def _stream_delta_dlt() -> DataFrame:
        return spark.readStream.format("delta").table(
            f"{task['src_catalog']}.{task['src_schema']}.{task['src_table']}"
        )

def stream_cloudFiles_dlt(spark, task):    
    @dlt.expect_all_or_drop(task['expectations'])
    @dlt.table(name=f"{task['tgt_table']}", comment=f"{task['tgt_table']}")
    def _stream_cloudFiles_dlt() -> DataFrame:
        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .load(task['src_path'])
        )

def read_parquet_dlt(spark, task):    
    @dlt.expect_all_or_drop(task['expectations'])
    @dlt.table(name=f"{task['tgt_table']}", comment=f"{task['tgt_table']}")
    def _read_parquet_dlt() -> DataFrame:
        return (
            spark.read.format("parquet")
                .option("cloudFiles.format", "parquet")
                .load(task['src_path'])
        )