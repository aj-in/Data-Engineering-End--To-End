# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

# Removing col

df = df.drop("_rescued_data")

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetearjun.dfs.core.windows.net/regions")

# COMMAND ----------



# COMMAND ----------

df = spark.read.format("delta").load("abfss://silver@databricksetearjun.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating table in "silver" schema 
# MAGIC
# MAGIC
# MAGIC create table if not exists databricks_cata.silver.regions_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksetearjun.dfs.core.windows.net/regions' 
# MAGIC