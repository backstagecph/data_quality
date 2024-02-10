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

delta_date = '2024-01-25'
datasource_name = "databricks"
asset_name = f"silver.order"
suite_name = "order_suite"
checkpoint_name = f"Delta Load Check: {delta_date}"
delta_column = "dw_load_datetime"

# COMMAND ----------

df_for_validation = spark.sql(f"""SELECT * FROM {asset_name} WHERE {delta_column} = '{delta_date}' LIMIT 1000""")

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
