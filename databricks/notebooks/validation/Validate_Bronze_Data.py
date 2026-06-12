# Databricks notebook source
# MAGIC %md
# MAGIC # Validate_Bronze_Data
# MAGIC
# MAGIC **Layer:** Validation
# MAGIC **Purpose:** Validates raw source data before loading into Bronze Delta tables.
# MAGIC Checks for schema conformance, null rates, row counts, and source-specific business rules.
# MAGIC
# MAGIC **Inputs:** Raw source data from ADLS raw-landing zone
# MAGIC **Outputs:** Validation pass/fail status written to control table
# MAGIC
# MAGIC **Usage in DAB job:** Called per source via `base_parameters: { source: <source_name> }`
# MAGIC
# MAGIC **Source notebook path (workspace):**
# MAGIC `/Users/shivakumaryallanti5@gmail.com/project customer 360/Validate_Bronze_Data`

# COMMAND ----------

# Parameters - passed via DAB job base_parameters
dbutils.widgets.text("source", "", "Source Name")
source = dbutils.widgets.get("source")
print(f"Validating source: {source}")

# COMMAND ----------

# TODO: Sync full notebook content from workspace
# To export this notebook from the Databricks workspace and commit it here:
#   1. In Databricks workspace, open the notebook
#   2. File > Export > Source File (.py)
#   3. Replace this file with the exported content
#   4. Commit and push to this branch

# COMMAND ----------

# The actual notebook logic is currently live in the Databricks workspace at:
# /Users/shivakumaryallanti5@gmail.com/project customer 360/Validate_Bronze_Data
# 
# Once exported and committed here, the DAB bundle will deploy it to:
# /Workspace/Shared/customer-360/{target}/files/notebooks/validation/Validate_Bronze_Data
