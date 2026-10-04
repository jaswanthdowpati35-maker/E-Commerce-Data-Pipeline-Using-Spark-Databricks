### Ingest Fact Data into Bronze Layer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "ecommcistorage05", "Storage Account Name")
dbutils.widgets.text("container_name", "ecomm-raw-data", "Container Name")
#### Widgets
catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

print(catalog_name, storage_account_name, container_name)
# -------------------------
# Azure Data Lake Storage - ADLS Gen2
# -------------------------

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/order_items/landing/"

# Checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_items/"

1. We are using Autoloader to perform incremental data processing.
1. Bronze layer is a data sink so it is append only. There are no updates on delete in the bronze layer.
1. Silver layer will take care of deduplications on duplicate files
1. Gold layer has analytics ready table
1. trigger(availableNow=True) is used to perform batch operation. You can stream the data as well if you have continuously arriving data and low letency requirements are there.
spark.readStream \
 .format("cloudFiles") \
 .option("cloudFiles.format", "csv")  \
 .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
 .option("cloudFiles.schemaEvolutionMode", "rescue") \
 .option("header", "true") \
 .option("cloudFiles.inferColumnTypes", "true") \
 .option("rescuedDataColumn", "_rescued_data") \
 .option("cloudFiles.includeExistingFiles", "true")  \
 .option("pathGlobFilter", "*.csv") \
 .load(adls_path) \
 .withColumn("ingest_timestamp", F.current_timestamp()) \
 .withColumn("source_file", F.col("_metadata.file_path")) \
 .writeStream \
 .outputMode("append") \
 .option("checkpointLocation", bronze_checkpoint_path) \
 .trigger(availableNow=True) \
 .toTable(f"{catalog_name}.bronze.brz_order_items") \
 .awaitTermination()
display(spark.sql(f"SELECT max(dt) FROM {catalog_name}.bronze.brz_order_items"))
display(spark.sql(f"SELECT min(dt) FROM {catalog_name}.bronze.brz_order_items"))
display(
    spark.sql(
        f"SELECT count(*) FROM CLOUD_FILES_STATE('abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_items/')"
    )
)
