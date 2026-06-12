# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Bronze to Delta append pipeline
# MAGIC
# MAGIC This notebook loads one Bronze source folder from ADLS and appends it into a Unity Catalog Delta table.
# MAGIC
# MAGIC It is split into small steps so the flow is easy to follow:
# MAGIC
# MAGIC * define imports and project defaults
# MAGIC * read runtime parameters
# MAGIC * build the target table name
# MAGIC * build and validate the source path
# MAGIC * read source files
# MAGIC * add bronze metadata columns
# MAGIC * append the data into the target Delta table

# COMMAND ----------

# DBTITLE 1,Imports and defaults
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime, date
import re

catalog = "dbw_c360_canadalife"
schema = "bronze"
supported_formats = {"csv", "json", "parquet"}
SOURCE_CONTRACTS = {
    "salesforce_crm": {"expected_prefixes": ["crm_customers_"], "expected_format": "csv"},
    "gwl_policy_admin": {"expected_prefixes": ["gwl_policies_"], "expected_format": "csv"},
    "ll_policy_admin": {"expected_prefixes": ["policies_"], "expected_format": "csv"},
    "sap_billing": {"expected_prefixes": ["billing_invoices_"], "expected_format": "csv"},
    "adobe_analytics": {"expected_prefixes": ["web_sessions_"], "expected_format": "json"},
    "avaya_call_centre": {"expected_prefixes": ["call_centre_interactions_", "interactions_"], "expected_format": "csv"},
    "group_benefits": {"expected_prefixes": ["group_members_"], "expected_format": "csv"},
    "f55_advisor": {"expected_prefixes": ["advisor_assignments_", "advisors_"], "expected_format": "csv"},
    "my_cl_portal": {"expected_prefixes": ["claims_"], "expected_format": "csv"},
    "climl_invest": {"expected_prefixes": ["investment_accounts_"], "expected_format": "csv"},
    "group_retirement": {"expected_prefixes": ["group_retirement_plan_members_", "plan_members_"], "expected_format": "csv"},
    "reinsurance": {"expected_prefixes": ["reinsurance_treaties_"], "expected_format": "csv"},
}

TARGET_TABLES = {
    "salesforce_crm": "salesforce_crm_bronze",
    "gwl_policy_admin": "gwl_policy_individual_life_bronze",
    "ll_policy_admin": "ll_policy_individual_life_bronze",
    "sap_billing": "sap_billing_invoices_bronze",
    "adobe_analytics": "adobe_analytics_digital_events_bronze",
    "avaya_call_centre": "call_centre_interactions_bronze",
    "group_benefits": "group_benefits_plan_members_bronze",
    "f55_advisor": "freedom55_advisor_assignments_bronze",
    "my_cl_portal": "portal_digital_events_bronze",
    "climl_invest": "climl_seg_fund_contracts_bronze",
    "group_retirement": "group_retirement_plan_members_bronze",
    "reinsurance": "reinsurance_treaty_data_bronze",
}


# COMMAND ----------

# DBTITLE 1,Runtime parameters
# This notebook is intended to run from a job.
# Each parallel task passes the source-specific runtime values.

SOURCE_NAME_ALIASES = {
    "salesforce.crm": "salesforce_crm",
    "gwl_policy.individual_life": "gwl_policy_admin",
    "ll_policy.individual_life": "ll_policy_admin",
    "sap_billing.invoices": "sap_billing",
    "adobe_analytics.digital_events": "adobe_analytics",
    "call_centre.interactions": "avaya_call_centre",
    "group_benefits.plan_members": "group_benefits",
    "freedom55.advisor_assignments": "f55_advisor",
    "portal.digital_events": "my_cl_portal",
    "climl.seg_fund_contracts": "climl_invest",
    "group_retirement.plan_members": "group_retirement",
    "reinsurance.treaty_data": "reinsurance",
}

dbutils.widgets.text("source_name", "")
dbutils.widgets.text("file_format", "csv")
dbutils.widgets.text("run_date", "")

source_name_input = dbutils.widgets.get("source_name").strip()
source_name = SOURCE_NAME_ALIASES.get(source_name_input, source_name_input)
file_format = dbutils.widgets.get("file_format").strip().lower()
run_date_input = dbutils.widgets.get("run_date").strip()

if run_date_input:
    try:
        run_date = datetime.strptime(run_date_input, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as error:
        raise ValueError("run_date must use YYYY-MM-DD format when provided") from error
else:
    run_date = date.today().strftime("%Y-%m-%d")


# COMMAND ----------

# DBTITLE 1,Validate inputs
missing_required_parameters = [
    parameter_name
    for parameter_name, parameter_value in {"source_name": source_name_input}.items()
    if not parameter_value
]

if missing_required_parameters:
    print("Notebook is configured for job-driven parallel source ingestion.")
    print(
        "Set the required runtime parameters before running manually: "
        + ", ".join(missing_required_parameters)
    )
    dbutils.notebook.exit(
        "SKIPPED: missing required runtime parameters - "
        + ", ".join(missing_required_parameters)
    )

if file_format not in supported_formats:
    raise ValueError(
        f"Unsupported file_format '{file_format}'. Use one of {sorted(supported_formats)}."
    )

if source_name not in SOURCE_CONTRACTS:
    raise ValueError(
        f"Unsupported source_name '{source_name_input}'. Add it to SOURCE_CONTRACTS or SOURCE_NAME_ALIASES first."
    )


# COMMAND ----------

# DBTITLE 1,Resolve target table
# Resolve the bronze target table for the logical source.
sanitized_source_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_name.lower())
table_name = TARGET_TABLES.get(source_name, f"{sanitized_source_name}_bronze")
full_table_identifier = f"{catalog}.{schema}.{table_name}"

print(f"Requested source_name : {source_name_input}")
print(f"Resolved source_name  : {source_name}")
print(f"Resolved run_date     : {run_date}")
print(f"Resolved file_format  : {file_format}")
print(f"Resolved target table : {full_table_identifier}")


# COMMAND ----------

# DBTITLE 1,Build source path
from datetime import timedelta

late_arrival_tolerance_days = 3
base_path = f"abfss://bronze@adlsc360canadalife.dfs.core.windows.net/{source_name}/"
parsed_run_date = datetime.strptime(run_date, "%Y-%m-%d")

bronze_path = None
for offset in range(late_arrival_tolerance_days + 1):
    candidate_date = parsed_run_date - timedelta(days=offset)
    candidate_path = (
        f"{base_path}year={candidate_date:%Y}/month={candidate_date:%m}/day={candidate_date:%d}/"
    )
    try:
        dbutils.fs.ls(candidate_path)
        bronze_path = candidate_path
        print(f"Found data at: {bronze_path}")
        break
    except Exception:
        continue

if bronze_path is None:
    raise FileNotFoundError(
        f"Source path does not exist in ADLS â check that ADF has landed files for "
        f"source_name='{source_name}' on run_date='{run_date}' (tolerance: {late_arrival_tolerance_days} days). "
        f"Expected path: {base_path}"
    )

print(f"Resolved source path : {bronze_path}")

def collect_files_recursively(root_path: str):
    files = []
    stack = [root_path]
    while stack:
        current_path = stack.pop()
        entries = dbutils.fs.ls(current_path)
        files.extend(entry for entry in entries if entry.isFile())
        stack.extend(entry.path for entry in entries if entry.isDir())
    return files


def detect_file_format_from_files(files):
    for file_info in files:
        file_name = file_info.name.lower()
        if file_name.endswith(".json"):
            return "json"
        if file_name.endswith(".csv"):
            return "csv"
        if file_name.endswith(".parquet"):
            return "parquet"
    raise ValueError("Unable to determine file format from bronze files")

source_files = collect_files_recursively(bronze_path)
if not source_files:
    raise FileNotFoundError(f"No files found under resolved bronze path: {bronze_path}")

expected_contract = SOURCE_CONTRACTS[source_name]
expected_prefixes = expected_contract["expected_prefixes"]
expected_format = expected_contract["expected_format"]
source_file_names = [file_info.name for file_info in source_files]

if not any(
    any(file_name.startswith(expected_prefix) for expected_prefix in expected_prefixes)
    for file_name in source_file_names
):
    raise ValueError(
        f"Resolved bronze path contains files that do not match the expected source contract for {source_name}. "
        f"Expected one of {expected_prefixes}, found files: {source_file_names}"
    )

effective_file_format = detect_file_format_from_files(source_files)
if effective_file_format != expected_format:
    raise ValueError(
        f"Resolved bronze path format mismatch for {source_name}. "
        f"Expected format '{expected_format}', detected '{effective_file_format}'."
    )

print(f"Expected filename prefixes: {expected_prefixes}")
print(f"Detected file format      : {effective_file_format}")

# COMMAND ----------

# DBTITLE 1,Validate source path exists
# Fail early if the expected landing path is missing in ADLS.
try:
    dbutils.fs.ls(bronze_path)
except Exception:
    raise FileNotFoundError(
        f"Source path does not exist in ADLS â check that ADF has landed files for "
        f"source_name='{source_name}' on run_date='{run_date}'. "
        f"Expected path: {bronze_path}"
    )


# COMMAND ----------

# DBTITLE 1,Read bronze files
reader = spark.read.format(effective_file_format)

if effective_file_format == "csv":
    reader = reader.option("header", "true").option("inferSchema", "true")
elif effective_file_format == "json":
    reader = reader.option("multiLine", "true")

df = reader.load(bronze_path)
print(f"Loaded source DataFrame for format: {effective_file_format}")


# COMMAND ----------

# DBTITLE 1,Add bronze metadata columns
# Keep the bronze layer append-only and preserve file-level lineage.
df_enriched = (
    df.withColumn("source_system", lit(source_name))
    .withColumn("processing_date", lit(run_date))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file_path", col("_metadata.file_path"))
)


# COMMAND ----------

# DBTITLE 1,Create schema if needed
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
print(f"Ensured schema exists: {catalog}.{schema}")


# COMMAND ----------

# DBTITLE 1,Append to Delta table
# Count before writing so the notebook can report the load size.
record_count = df_enriched.count()
write_mode = "append"
action_message = "Appended data to"

try:
    spark.table(full_table_identifier).limit(1).collect()
except Exception as error:
    recoverable_errors = (
        "DELTA_TABLE_NOT_FOUND",
        "DELTA_PATH_DOES_NOT_EXIST",
        "TABLE_OR_VIEW_NOT_FOUND",
    )
    if any(error_code in str(error) for error_code in recoverable_errors):
        write_mode = "overwrite"
        action_message = "Recreated and loaded data into"
        print(f"Target table is unavailable; recreating {full_table_identifier}")
    else:
        raise

writer = df_enriched.write.format("delta").mode(write_mode)
if write_mode == "append":
    writer = writer.option("mergeSchema", "true")
else:
    writer = writer.option("overwriteSchema", "true")

writer.saveAsTable(full_table_identifier)

print(f"Loaded {record_count} records from {bronze_path}")
print(f"{action_message}: {full_table_identifier}")
dbutils.notebook.exit("SUCCESS")
