# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze_Delta_tables
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Purpose:** Ingests raw data into Bronze Delta tables.
# MAGIC
# MAGIC **Source path:** `/Users/shivakumaryallanti5@gmail.com/project customer 360/Bronze_Delta_tables`

# COMMAND ----------

dbutils.widgets.text("source", "", "Source Name")
source = dbutils.widgets.get("source")
print(f"Loading source into Bronze Delta: {source}")

# COMMAND ----------

# TODO: Export notebook from workspace and replace this file.
