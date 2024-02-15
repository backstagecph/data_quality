# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC pip install -e git+https://github.com/leet1447/great_expectations#egg=great_expectations
# MAGIC pip install azure-storage-blob

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import great_expectations as gx

context = gx.get_context(context_root_dir = './../gx')

# COMMAND ----------

datasource_name = "databricks"
asset_name = f"demo.orders"
suite_name = "demo_orders_suite"
delta_column = "dw_datetime_load_utc"
delta_date = "2024-02-14"
checkpoint_name = f"Delta Load Check: {delta_date}"

# COMMAND ----------

if delta_column != "":
    df_for_validation = spark.sql(f"""SELECT * FROM {asset_name} WHERE {delta_column} = '{delta_date}' LIMIT 1000""")
else:
    df_for_validation = spark.sql(f"""SELECT * FROM {asset_name}""")

# COMMAND ----------

datasource = context.get_datasource(datasource_name=datasource_name)

# COMMAND ----------

data_asset = datasource.get_asset(asset_name=asset_name)

# COMMAND ----------

data_asset.build_batch_request(dataframe=df_for_validation)

# COMMAND ----------

batch_request = data_asset.build_batch_request(dataframe = df_for_validation)

# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name=checkpoint_name,
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

# COMMAND ----------

checkpoint.run(run_name=f"{checkpoint_name}")

# COMMAND ----------

context.build_data_docs(site_names="azure_hosted_site")

# COMMAND ----------


