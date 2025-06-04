from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count, rank

spark = SparkSession.builder \
    .appName("TopDomainsPerCategory") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .getOrCreate()

jdbc_url = "jdbc:mysql://mysql:3306/smart_ads"
properties = {"user": "root", "password": "root"}

df = spark.read.jdbc(jdbc_url, "log_voucher", properties=properties)

agg_df = df.groupBy("new_category", "domain").agg(count("*").alias("access_count"))

window = Window.partitionBy("new_category").orderBy(col("access_count").desc())
ranked_df = agg_df.withColumn("rank", rank().over(window)).filter(col("rank") <= 5)

output_path = "/lakehouse/gold/top_domains_by_category"
table_name = "top_domains_by_category"

ranked_df.write.format("hudi") \
    .option("hoodie.table.name", table_name) \
    .option("hoodie.datasource.write.recordkey.field", "domain") \
    .option("hoodie.datasource.write.precombine.field", "access_count") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "upsert").mode("append").save(output_path)

print("Da ghi voa Hudi table.")
