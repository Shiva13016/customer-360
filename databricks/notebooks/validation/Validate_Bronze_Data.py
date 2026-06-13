# Databricks notebook source
# DBTITLE 1,Notebook overview
# MAGIC %md
# MAGIC This notebook validates bronze-layer file delivery before downstream processing starts.
# MAGIC
# MAGIC It is designed to act as a hard gate:
# MAGIC * Requires only `source_name`
# MAGIC * Uses the system date at runtime
# MAGIC * Supports a 3-day late-arrival tolerance window
# MAGIC * Accepts an optional `run_date` override for deterministic backfills and replays
# MAGIC * Fails the notebook task when no valid files are found or when delivered files are empty
# MAGIC
# MAGIC Execution flow:
# MAGIC * Setup parameters and validation window
# MAGIC * Define reusable helper functions
# MAGIC * Validate the delivery folder and compute quality checks
# MAGIC * Return success details or fail the task

# COMMAND ----------

# DBTITLE 1,Setup parameters
from datetime import datetime, date, timedelta

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

# Required input
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("run_date", "")

source_name_input = dbutils.widgets.get("source_name").strip()
run_date_input = dbutils.widgets.get("run_date").strip()
source_name = SOURCE_NAME_ALIASES.get(source_name_input, source_name_input)

if not source_name_input:
    raise ValueError("source_name parameter is required")
if source_name not in SOURCE_CONTRACTS:
    raise ValueError(
        f"Unsupported source_name '{source_name_input}'. Add it to SOURCE_CONTRACTS or SOURCE_NAME_ALIASES first."
    )

if run_date_input:
    try:
        system_date = datetime.strptime(run_date_input, "%Y-%m-%d").date()
    except ValueError as error:
        raise ValueError("run_date must use YYYY-MM-DD format when provided") from error
else:
    system_date = date.today()

late_arrival_tolerance_days = 3
validation_dates = [
    system_date - timedelta(days=offset)
    for offset in range(late_arrival_tolerance_days + 1)
]

base_path = f"abfss://bronze@adlsc360canadalife.dfs.core.windows.net/{source_name}/"

print(f"Requested source name: {source_name_input}")
print(f"Resolved source name : {source_name}")
print(f"Base path            : {base_path}")
print(f"Effective run date   : {system_date.strftime('%Y/%m/%d')}")
print(
    "Accepted delivery dates: "
    + ", ".join(validation_date.strftime('%Y/%m/%d') for validation_date in validation_dates)
)

# COMMAND ----------

# DBTITLE 1,Helper functions
def detect_file_format(files):
    for file_info in files:
        file_name = file_info.name.lower()
        if file_name.endswith(".json"):
            return "json"
        if file_name.endswith(".csv"):
            return "csv"
        if file_name.endswith(".parquet"):
            return "parquet"
    raise ValueError("Unable to determine file format from delivered files")


def read_files_for_format(file_format: str, file_paths):
    if file_format == "json":
        return spark.read.option("multiLine", "true").json(file_paths)
    if file_format == "parquet":
        return spark.read.parquet(*file_paths)
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(file_paths)
    )


def build_candidate_delivery_paths(root_path: str, delivery_date: date):
    year_part = f"year={delivery_date.year}"
    month_parts = [f"month={delivery_date.month:02d}", f"month={delivery_date.month}"]
    day_parts = [f"day={delivery_date.day:02d}", f"day={delivery_date.day}"]

    candidates = []
    for month_part in month_parts:
        for day_part in day_parts:
            candidate = f"{root_path}{year_part}/{month_part}/{day_part}/"
            if candidate not in candidates:
                candidates.append(candidate)
    return candidates


def collect_files_recursively(root_path: str):
    files = []
    stack = [root_path]

    while stack:
        current_path = stack.pop()
        entries = dbutils.fs.ls(current_path)
        files.extend(entry for entry in entries if entry.isFile())
        stack.extend(entry.path for entry in entries if entry.isDir())

    return files


def find_delivery_path(root_path: str, delivery_dates):
    checked_paths = []

    for delivery_date in delivery_dates:
        candidate_paths = build_candidate_delivery_paths(root_path, delivery_date)
        for candidate_path in candidate_paths:
            checked_paths.append(candidate_path)
            try:
                files = collect_files_recursively(candidate_path)
                if files:
                    return delivery_date, candidate_path, files, checked_paths
            except Exception:
                continue

    checked_dates = ", ".join(
        delivery_date.strftime('%Y/%m/%d') for delivery_date in delivery_dates
    )
    raise FileNotFoundError(
        f"No files found for delivery dates [{checked_dates}] under {root_path}. "
        f"Checked paths: {', '.join(checked_paths)}"
    )

# COMMAND ----------

# DBTITLE 1,Run bronze validation
print(f"--- Starting Validation for {source_name} ---")

try:
    matched_date, delivery_path, delivered_files, checked_paths = find_delivery_path(
        base_path, validation_dates
    )
    file_format = detect_file_format(delivered_files)
    expected_contract = SOURCE_CONTRACTS[source_name]
    expected_prefixes = expected_contract["expected_prefixes"]
    expected_format = expected_contract["expected_format"]
    delivered_file_names = [file_info.name for file_info in delivered_files]

    if not any(
        any(file_name.startswith(expected_prefix) for expected_prefix in expected_prefixes)
        for file_name in delivered_file_names
    ):
        raise ValueError(
            f"Validation FAILED: expected one of filename prefixes {expected_prefixes} for {source_name}, "
            f"but found files: {delivered_file_names}"
        )
    if file_format != expected_format:
        raise ValueError(
            f"Validation FAILED: expected file format '{expected_format}' for {source_name}, detected '{file_format}'"
        )

    file_paths = [file_info.path for file_info in delivered_files]
    df = read_files_for_format(file_format, file_paths)

    row_count = df.count()
    file_count = len(delivered_files)

    latest_file = max(
        delivered_files,
        key=lambda file_info: getattr(file_info, "modificationTime", 0),
    )
    latest_file_name = latest_file.name
    latest_file_path = latest_file.path
    latest_file_row_count = read_files_for_format(file_format, [latest_file_path]).count()

    print(f"Checked delivery paths: {checked_paths}")
    print(f"Matched delivery date: {matched_date.strftime('%Y/%m/%d')}")
    print(f"Matched delivery path: {delivery_path}")
    print(f"Detected format: {file_format}")
    print(f"Expected filename prefixes: {expected_prefixes}")
    print(f"File count: {file_count}")
    print(f"Total row count: {row_count}")
    print(f"Latest file name: {latest_file_name}")
    print(f"Latest file row count: {latest_file_row_count}")

    if row_count == 0:
        raise ValueError(
            "Validation FAILED: Delivered files exist for the accepted delivery window but contain 0 rows."
        )

    result_msg = (
        f"SUCCESS|{matched_date.strftime('%Y/%m/%d')}|{row_count}|{file_count}|"
        f"{latest_file_name}|{file_format}|{latest_file_row_count}"
    )
    print(result_msg)
    dbutils.notebook.exit(result_msg)

except Exception as e:
    result_msg = f"FAILURE|{str(e)}"
    print(result_msg)
    raise RuntimeError(result_msg) from e
