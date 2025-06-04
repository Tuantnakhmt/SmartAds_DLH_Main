from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder \
    .appName("Hudi Sample Write") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .getOrCreate()

df = spark.read.option("header", True).csv("/data/domain_data.csv") \
    .withColumn("ts", current_timestamp())

hudi_table_path = "/lakehouse/bronze/domain_data_hudi"
hudi_table_name = "domain_data_hudi"

df.write.format("hudi") \
    .option("hoodie.table.name", hudi_table_name) \
    .option("hoodie.datasource.write.recordkey.field", "domain") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .mode("overwrite") \
    .save(hudi_table_path)

print(f"✅ Hudi table written to {hudi_table_path}")
