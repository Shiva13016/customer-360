# Databricks notebook source
# MAGIC %md
# MAGIC # Compliance_Audit_Notebook
# MAGIC
# MAGIC **Layer:** Validation Gate / Audit
# MAGIC **Purpose:** Audit gate notebook used at multiple stages of the C-360 pipeline.
# MAGIC Logs validation results, checks compliance, and acts as a barrier task between layers.
# MAGIC
# MAGIC **Usage in DAB job:**
# MAGIC - After all_validations_complete gate (no params)
# MAGIC - After all_bronze_complete gate (base_parameters: { layer: bronze })
# MAGIC
# MAGIC **Source notebook path (workspace):**
# MAGIC `/Users/shivakumaryallanti5@gmail.com/project customer 360/Compliance Audit Notebook`

# COMMAND ----------

dbutils.widgets.text("layer", "", "Layer")
layer = dbutils.widgets.get("layer")
print(f"Compliance audit for layer: {layer if layer else 'pre-bronze validation gate'}")

# COMMAND ----------

# TODO: Export full notebook from workspace and replace this file.
# Source: /Users/shivakumaryallanti5@gmail.com/project customer 360/Compliance Audit Notebook
