from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("job_1") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

df = spark.read.csv("s3a://data-bucket/orders.csv", header=True)

result = df.groupBy("customer_id").count()

result.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="customer_orders", keyspace="bigdata") \
    .mode("overwrite") \
    .save()

spark.stop()