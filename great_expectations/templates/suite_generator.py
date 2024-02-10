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
datasource_name = "databricks"
asset_name = f"silver.order"
suite_name = "order_suite"

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

validator.expect_column_values_to_be_null("payment_type")

validator.expect_column_values_to_not_be_null("gift")

validator.expect_compound_columns_to_be_unique(["", "",""])

validator.expect_column_values_to_be_in_set("bundle_split", ["0","1","2"])

# COMMAND ----------

validator.save_expectation_suite()
