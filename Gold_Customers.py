# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

init_load_flag =int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

#Data Reading


df = spark.sql("select * from databricks_cata.silver.customers_silver")

df.display()


# COMMAND ----------

# Removing the duplicates based on the primary key 


df= df.dropDuplicates(subset=['customer_id'])
df.limit(10).display()


# COMMAND ----------

# Dividing old vs new records 


if init_load_flag ==0:                  #if table is present 
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date from databricks_cata.gold.DimCustomers''')

else: 
    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date from databricks_cata.silver.customers_silver where 1=0''')


# Make joins if there are null they are new reocrds 



# COMMAND ----------

df_old.display()

# COMMAND ----------

# Renaming columns of df_old

df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
         .withColumnRenamed("customer_id", "old_customer_id")\
         .withColumnRenamed("create_date", "old_create_date")\
         .withColumnRenamed("update_date", "old_update_date")



# COMMAND ----------

# MAGIC %md
# MAGIC Applying Join with old records  

# COMMAND ----------


df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], 'left')


# COMMAND ----------

df_join.display()

# COMMAND ----------

# Seperating old vs new records 

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())  # new ones will have empty values

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull()) 

# COMMAND ----------

df_old.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing df_old

# COMMAND ----------

# Dropping cols

df_old = df_old.drop('old_customer_id', 'old_update_date')

df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")


# Renaming 
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col('create_date')))


# adding update date col
# Create date does not need to be changed in the final data, update date needs to be changed
# Giving latest values in update date

df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Preaparing new_df records for insert

# COMMAND ----------

# Dropping cols

df_new = df_new.drop('old_DimCustomerKey','old_customer_id', 'old_update_date', 'old_create_date')



# adding update date col
# Create date does not need to be changed in the final data, update date needs to be changed
# Giving latest values in update date

df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())
df_new.display()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Surrogate Key from one

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))

#+1 for ids starting from 1, 0 start by defautl 

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding max surrogate key**

# COMMAND ----------

if init_load_flag ==1:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cata.gold.DimCustomers")
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']   #converting df_max_sur to max_surrogate_key

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key)+col("DimCustomerKey"))
df_new.display()

# COMMAND ----------

# Union of df_old and df_new 

df_final = df_new.unionByName(df_old)
df_final.display()

# COMMAND ----------

# SCD Type -1 

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.DimCustomers"):


    dlt_obj = DeltaTable.forPath(spark,"abfss://gold@databrickscata.dfs.core.windows.net/DimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()



else:
    
    df_final.write.mode("overwrite")\
    .format("delta")\
    .option("path", "abfss://gold@databricksetearjun.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("databricks_cata.gold.DimCustomers")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimcustomers

# COMMAND ----------



# COMMAND ----------

