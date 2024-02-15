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
asset_name = f"demo.orders"
suite_name = "demo_orders_suite"

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
validator.expect_column_to_exist("sk_transaction_header")
validator.expect_column_to_exist("sk_transaction_line")
validator.expect_compound_columns_to_be_unique(["sk_transaction_header", "sk_transaction_line"])

# Rule on 'payment_type': should not in a set and not null
validator.expect_column_to_exist("payment_type")
validator.expect_column_values_to_be_in_set("payment_type", ["Credit Card", "Cash", "PayPal"])
validator.expect_column_values_to_not_be_null("payment_type")

# COMMAND ----------

validator.save_expectation_suite()

# COMMAND ----------


