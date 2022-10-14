from pyspark.sql import SparkSession
from kensu.pyspark import init_kensu_spark

# download kensu spark collector jar if doesn't exist in ../lib/
from spark_collector_downloader import maybe_download_spark_collector, kensu_agent_jar_local_path
maybe_download_spark_collector(kensu_agent_jar_local_path)


#Add the path to the .jar to the SparkSession
spark = SparkSession.builder.appName("Example")\
    .config("spark.driver.extraClassPath", kensu_agent_jar_local_path)\
    .getOrCreate()

#Init Kensu

init_kensu_spark(spark)


business_info = spark.read.option("inferSchema","true").option("header","true").csv("data/business-data.csv")
customers_info = spark.read.option("inferSchema","true").option("header","true").csv("data/customers-data.csv")
contact_info = spark.read.option("inferSchema","true").option("header","true").csv("data/contact-data.csv")


df = business_info.join(customers_info,['id']).join(contact_info,['id'])

df.write.mode("overwrite").save("data/data")