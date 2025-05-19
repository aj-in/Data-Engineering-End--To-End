# Databricks notebook source
# MAGIC %md
# MAGIC Data Reading 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import * 
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@databricksetearjun.dfs.core.windows.net/orders")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()


# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

#Getting Timestamp

df = df.withColumn("order_date",to_timestamp(col('order_date')))

df.display()

# COMMAND ----------

# Extracting year from date

df = df.withColumn("year",year(col('order_date')))

df.display()

# COMMAND ----------

# Group By 

df1 = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

df1.display()

# COMMAND ----------

# Group By 

df2 = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

df2.display()

# COMMAND ----------

# Group By 

df3 = df.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

df3.display()

# COMMAND ----------

# Classes

class windows:

    def dense_rank(self, df):
        df_dense_rank = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

        return df_dense_rank
    
    def rank(self, df):
        df_rank = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_rank

    def row_number(self, df):
        df_row_number = df.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

        return df_row_number

# COMMAND ----------

df_new = df

df_new.display()

# COMMAND ----------

obj = windows()

df_new1 = obj.dense_rank(df_new)


# COMMAND ----------

df_new1.display()


# COMMAND ----------

# Data Writing

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetearjun.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating table in "silver" schema 
# MAGIC
# MAGIC
# MAGIC create table if not exists databricks_cata.silver.orders_silver 
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksetearjun.dfs.core.windows.net/orders' 
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

