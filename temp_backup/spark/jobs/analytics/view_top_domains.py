from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ViewTopDomains") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

# ✅ Read Hudi output
df = spark.read.format("hudi").load("/lakehouse/gold/top_domains_by_category")
df.orderBy("new_category", "rank").show(50, truncate=False)