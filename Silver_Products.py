# Databricks notebook source
# Data loading 


from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@databricksetearjun.dfs.core.windows.net/products")


df.display()

# COMMAND ----------

# Removing columns

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Views**

# COMMAND ----------

df.createOrReplaceTempView("products")
# spark df avaiable to use  as "products" by SQL .....view

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- independent fucntion for 10% percent discount
# MAGIC
# MAGIC create or replace function databricks_cata.bronze.discount_func(p_price double)
# MAGIC returns double 
# MAGIC language sql
# MAGIC return p_price * 0.90                
# MAGIC
# MAGIC
# MAGIC -- Naming convention:  databricks_cata.bronze  == catalog name.schema name 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price,databricks_cata.bronze.discount_func(price) as discounted_price 
# MAGIC from products

# COMMAND ----------

df= df.withColumn("discounted_price", expr("databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.upper_func(p_brand string)
# MAGIC returns string 
# MAGIC language python
# MAGIC
# MAGIC as 
# MAGIC $$ 
# MAGIC   return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select  product_id, brand, databricks_cata.bronze.upper_func(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path", "abfss://silver@databricksetearjun.dfs.core.windows.net/products").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating table in "silver" schema 
# MAGIC
# MAGIC
# MAGIC create table if not exists databricks_cata.silver.products_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksetearjun.dfs.core.windows.net/products' 
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

