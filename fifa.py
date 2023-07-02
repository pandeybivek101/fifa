# Databricks notebook source


# COMMAND ----------

def mount_container(container_name):
    source = f'wasbs://{container_name}@data0migration0p0dl.blob.core.windows.net'
    mount_point=f'/mnt/{container_name}'
    return dbutils.fs.mount(
  source = source,
  mount_point = mount_point,
  extra_configs = {"fs.azure.account.key.data0migration0p0dl.blob.core.windows.net":"szX/AOIKY+he7RFp1ze/lUUaD4p45AQX99jDgACsZEz2PQx6u/zqau+4yg0wjd7nNz/XWh6ft8G4+AStgCh++w=="})



# COMMAND ----------

mount_container('raw')

# COMMAND ----------

df=spark.read.csv('/mnt/raw/fifa.csv', header=True)

# COMMAND ----------

display(df)

# COMMAND ----------


