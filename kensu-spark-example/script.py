from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Example")\
    .getOrCreate()


business_info = spark.read.option("inferSchema","true").option("header","true").csv("data/business-data.csv")
customers_info = spark.read.option("inferSchema","true").option("header","true").csv("data/customers-data.csv")
contact_info = spark.read.option("inferSchema","true").option("header","true").csv("data/contact-data.csv")


df = business_info.join(customers_info,['id']).join(contact_info,['id'])

df.write.mode("overwrite").save("data/data")