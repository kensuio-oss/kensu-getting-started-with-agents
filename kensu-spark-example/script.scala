import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Example")
  .getOrCreate()

val business_info = spark.read.option("inferSchema","true").option("header","true").csv("data/business-data.csv")
val customers_info = spark.read.option("inferSchema","true").option("header","true").csv("data/customers-data.csv")
val contact_info = spark.read.option("inferSchema","true").option("header","true").csv("data/contact-data.csv")

val df = business_info.join(customers_info,business_info("id") === customers_info("id"), "inner").drop(customers_info("id"))
val df_final = df.join(contact_info,df("id") === contact_info("id"), "inner").drop(contact_info("id"))

df_final.write.mode("overwrite").save("data/data")