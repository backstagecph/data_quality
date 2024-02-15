# Databricks notebook source
# MAGIC %sh
# MAGIC
# MAGIC pip install -e git+https://github.com/leet1447/great_expectations#egg=great_expectations

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import great_expectations as gx

# COMMAND ----------

context = gx.get_context(context_root_dir = "./../gx")

# COMMAND ----------

# DBTITLE 1,Specify your parameters here!
datasource_name = ""
asset_name = f""
suite_name = ""

# COMMAND ----------

df_checks = spark.sql(f"""SELECT * FROM {asset_name} LIMIT 1000""")

# COMMAND ----------

datasource = context.sources.add_or_update_spark(datasource_name)

# COMMAND ----------

data_asset = datasource.add_dataframe_asset(name = asset_name)

# COMMAND ----------

context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# COMMAND ----------

batch_request = data_asset.build_batch_request(dataframe = df_checks)

# COMMAND ----------

validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# COMMAND ----------

# Rules on 'id': unique and not null
validator.expect_column_to_exist("id")
validator.expect_column_values_to_be_unique("id")
validator.expect_column_values_to_not_be_null("id")

# COMMAND ----------

validator.save_expectation_suite()

# COMMAND ----------


