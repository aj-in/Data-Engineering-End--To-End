# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


# COMMAND ----------

# Loading main data file 

df = spark.read.format("parquet").load("abfss://bronze@databricksetearjun.dfs.core.windows.net/customers")

df.display()

# COMMAND ----------

# Droppping columns 

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# Extracting domain from email 

df= df.withColumn("domains", split(col("email"), "@")[1])

df.display()

# COMMAND ----------

df.groupBy("domains") \
  .agg(count("customer_id").alias("total_customers")) \
  .orderBy(desc("total_customers")) \
  .display()

# COMMAND ----------

df_gmail = df.filter(df.domains == "gmail.com")
df_gmail.display()

df_hotmail = df.filter(df.domains == "hotmail.com")
df_hotmail.display()


df_yahoo = df.filter(df.domains == "yahoo.com")
df_yahoo.display()



# COMMAND ----------

# Merging names

df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))  

df= df.drop("first_name", "last_name")

df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@databricksetearjun.dfs.core.windows.net/customers")


# We are overwriting bevause we are not keeping data for silver layer, this is in a transient layer  

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating table in "silver" schema 
# MAGIC
# MAGIC
# MAGIC create table if not exists databricks_cata.silver.customers_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksetearjun.dfs.core.windows.net/customers' 
# MAGIC

# COMMAND ----------

