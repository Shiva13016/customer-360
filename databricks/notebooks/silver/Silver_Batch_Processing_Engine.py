# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC This notebook builds the Canada Life Customer 360 silver layer from bronze sources.
# MAGIC
# MAGIC Sections:
# MAGIC * Parameters and imports
# MAGIC * Configuration and target metadata
# MAGIC * Shared utility functions
# MAGIC * Source normalization helpers
# MAGIC * Target builders
# MAGIC * Execution orchestration
# MAGIC
# MAGIC Execution modes:
# MAGIC * `PLAN` inspects configured targets without reading full outputs
# MAGIC * `TEST` builds each target and previews rows without writing
# MAGIC * `RUN` builds and writes silver outputs to Unity Catalog and ADLS
# MAGIC
# MAGIC Active runtime widgets:
# MAGIC * `target_table_name`: choose one configured silver target to run, or use `ALL` to run the full silver pipeline
# MAGIC * `run_date`: optional incremental date filter applied to bronze inputs using `processing_date` or `ingestion_timestamp`
# MAGIC * `execution_mode`: controls whether the notebook plans, tests, or writes outputs
# MAGIC * `optimize_output`: when `true`, runs `OPTIMIZE` on written silver Delta tables in `RUN` mode
# MAGIC * `dq_threshold_pct`: maximum allowed null percentage for required columns in data quality gates
# MAGIC
# MAGIC Why these widgets remain:
# MAGIC * They support production control, selective reruns, and troubleshooting
# MAGIC * They do not control notebook dependencies; upstream bronze task completion is handled by the job DAG
# MAGIC

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC This notebook builds the Canada Life Customer 360 silver layer from bronze sources.
# MAGIC
# MAGIC Sections:
# MAGIC * Parameters and imports
# MAGIC * Configuration and target metadata
# MAGIC * Shared utility functions
# MAGIC * Source normalization helpers
# MAGIC * Target builders
# MAGIC * Execution orchestration
# MAGIC
# MAGIC Execution modes:
# MAGIC * `PLAN` inspects configured targets without reading full outputs
# MAGIC * `TEST` builds each target and previews rows without writing
# MAGIC * `RUN` builds and writes silver outputs to Unity Catalog and ADLS
# MAGIC
# MAGIC Active runtime widgets:
# MAGIC * `target_table_name`: choose one configured silver target to run, or use `ALL` to run the full silver pipeline
# MAGIC * `run_date`: optional incremental date filter applied to bronze inputs using `processing_date` or `ingestion_timestamp`
# MAGIC * `execution_mode`: controls whether the notebook plans, tests, or writes outputs
# MAGIC * `optimize_output`: when `true`, runs `OPTIMIZE` on written silver Delta tables in `RUN` mode
# MAGIC * `dq_threshold_pct`: maximum allowed null percentage for required columns in data quality gates
# MAGIC
# MAGIC Why these widgets remain:
# MAGIC * They support production control, selective reruns, and troubleshooting
# MAGIC * They do not control notebook dependencies; upstream bronze task completion is handled by the job DAG
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parameters and imports
# ==============================================================================
# Notebook: Silver_Batch_Processing_Engine
# Purpose   : Dynamic Bronze -> Silver batch processing framework for Canada Life
#             Customer 360 with 21 silver outputs from 12 bronze tables.
# Notes     :
#             * Adobe bronze is line-broken JSON stored in a single string column.
#               Only that source is JSON-flattened.
#             * Other flattening steps are string/array explodes, not JSON flattening.
#             * Default execution_mode is PLAN so the notebook is safe to run first.
# ==============================================================================
import re
import uuid
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ------------------------------------------------------------------------------
# 1. Widget Parameters
# ------------------------------------------------------------------------------
for widget_name in [
    "target_table_name",
    "run_date",
    "execution_mode",
    "optimize_output",
    "dq_threshold_pct",
]:
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

DEFAULT_CATALOG_NAME = "dbw_c360_canadalife"
DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_SILVER_BASE_PATH = "abfss://silver@adlsc360canadalife.dfs.core.windows.net"

widget_defaults = {
    "target_table_name": "ALL",
    "run_date": "",
    "execution_mode": "PLAN",   # PLAN | TEST | RUN
    "optimize_output": "false",
    "dq_threshold_pct": "2.0",
}

for widget_name, default_value in widget_defaults.items():
    dbutils.widgets.text(widget_name, default_value)

catalog_name = DEFAULT_CATALOG_NAME
bronze_schema = DEFAULT_BRONZE_SCHEMA
silver_schema = DEFAULT_SILVER_SCHEMA
target_table_name = dbutils.widgets.get("target_table_name").strip() or "ALL"
run_date = dbutils.widgets.get("run_date").strip()
silver_base_path = DEFAULT_SILVER_BASE_PATH
execution_mode = (dbutils.widgets.get("execution_mode").strip() or "PLAN").upper()
optimize_output = dbutils.widgets.get("optimize_output").strip().lower() == "true"
dq_threshold_pct = float(dbutils.widgets.get("dq_threshold_pct").strip() or "2.0") / 100.0
run_id = str(uuid.uuid4())

if execution_mode not in {"PLAN", "TEST", "RUN"}:
    raise ValueError("execution_mode must be one of PLAN, TEST, or RUN")


# COMMAND ----------

# DBTITLE 1,Configuration summary
# MAGIC %md
# MAGIC This section centralizes notebook metadata:
# MAGIC * bronze source table mappings
# MAGIC * expected source schemas for drift checks
# MAGIC * target definitions and write modes
# MAGIC * lookup dataframes and runtime caches
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuration and target metadata
# ------------------------------------------------------------------------------
# 2. Constants and Configuration
# ------------------------------------------------------------------------------
BRONZE_TABLES = {
    "salesforce.crm": "salesforce_crm_bronze",
    "ll_policy.individual_life": "ll_policy_individual_life_bronze",
    "gwl_policy.individual_life": "gwl_policy_individual_life_bronze",
    "sap_billing.invoices": "sap_billing_invoices_bronze",
    "climl.seg_fund_contracts": "climl_seg_fund_contracts_bronze",
    "call_centre.interactions": "call_centre_interactions_bronze",
    "group_benefits.plan_members": "group_benefits_plan_members_bronze",
    "freedom55.advisor_assignments": "freedom55_advisor_assignments_bronze",
    "portal.digital_events": "portal_digital_events_bronze",
    "reinsurance.treaty_data": "reinsurance_treaty_data_bronze",
    "group_retirement.plan_members": "group_retirement_plan_members_bronze",
    "adobe_analytics.digital_events": "adobe_analytics_digital_events_bronze",
}

SOURCE_EXPECTED_COLUMNS = {
    "salesforce.crm": {"customer_id", "email", "phone", "province", "postal_code", "source_system", "processing_date", "ingestion_timestamp"},
    "ll_policy.individual_life": {"policy_id", "customer_id", "product_code", "face_amount", "premium", "frequency", "issue_date", "status", "province", "rider_codes", "source_system", "processing_date", "ingestion_timestamp"},
    "gwl_policy.individual_life": {"gwl_policy_id", "customer_id", "product", "sum_assured", "premium", "frequency", "issue_year", "status", "province", "source_system", "processing_date", "ingestion_timestamp"},
    "sap_billing.invoices": {"invoice_id", "policy_id", "customer_id", "billing_date", "amount", "status", "source_system", "processing_date", "ingestion_timestamp"},
    "climl.seg_fund_contracts": {"account_id", "customer_id", "asset_type", "fund_code", "market_value", "purchase_date", "source_system", "processing_date", "ingestion_timestamp"},
    "call_centre.interactions": {"interaction_id", "customer_id", "agent_id", "call_start_ts", "call_end_ts", "channel", "issue_type", "resolution_status", "source_system", "processing_date", "ingestion_timestamp"},
    "group_benefits.plan_members": {"member_id", "plan_id", "coverage_type", "province", "effective_date", "termination_date", "source_system", "processing_date", "ingestion_timestamp"},
    "freedom55.advisor_assignments": {"advisor_id", "customer_id", "assignment_id", "assignment_start_date", "assignment_end_date", "region", "channel", "status", "source_system", "processing_date", "ingestion_timestamp"},
    "portal.digital_events": {"claim_id", "policy_id", "customer_id", "claim_type", "claim_date", "status", "source_system", "processing_date", "ingestion_timestamp"},
    "reinsurance.treaty_data": {"treaty_id", "policy_id", "reinsurer", "ceded_amount", "status", "source_system", "processing_date", "ingestion_timestamp"},
    "group_retirement.plan_members": {"member_id", "plan_id", "employer_id", "retirement_date", "contribution_amount", "vesting_status", "member_status", "source_system", "processing_date", "ingestion_timestamp"},
    "adobe_analytics.digital_events": {"event_id", "visitor_id", "session_id", "event_timestamp", "event_type", "page_name", "device_type", "campaign_id", "source_system", "processing_date", "ingestion_timestamp", "source_file_path"},
}

SCD2_TRACKED_COLUMNS = [
    "policy_status_code",
    "face_amount",
    "premium_amount",
    "premium_frequency_code",
    "beneficiary_id",
    "underwriting_class_code",
    "rider_codes",
    "occupational_class_code",
    "dividend_option_code",
    "smoker_status_code",
]

TARGET_CONFIG = {
    "customer.master": {"kind": "business", "sources": ["salesforce.crm", "gwl_policy.individual_life", "sap_billing.invoices", "policy.individual_life_clean"], "keys": ["customer_id"], "write_mode": "overwrite"},
    "policy.individual_life_clean": {"kind": "business", "sources": ["ll_policy.individual_life", "gwl_policy.individual_life", "salesforce.crm"], "keys": ["policy_number"], "write_mode": "scd2"},
    "policy.individual_life_enriched": {"kind": "business", "sources": ["policy.individual_life_clean"], "keys": ["policy_number", "effective_date"], "write_mode": "overwrite"},
    "policy.disability_ci_clean": {"kind": "business", "sources": ["ll_policy.individual_life"], "keys": ["policy_number"], "write_mode": "overwrite"},
    "policy.policy_rider_detail": {"kind": "business", "sources": ["policy.individual_life_enriched"], "keys": ["policy_number", "rider_code"], "write_mode": "overwrite"},
    "digital.portal_clean": {"kind": "business", "sources": ["adobe_analytics.digital_events", "portal.digital_events"], "keys": ["event_id"], "write_mode": "overwrite"},
    "interactions.callcentre_clean": {"kind": "business", "sources": ["call_centre.interactions"], "keys": ["interaction_id"], "write_mode": "overwrite"},
    "group_benefits.plan_clean": {"kind": "business", "sources": ["group_benefits.plan_members"], "keys": ["plan_id", "member_id"], "write_mode": "overwrite"},
    "group_benefits.certificate_clean": {"kind": "business", "sources": ["group_benefits.plan_members"], "keys": ["certificate_number"], "write_mode": "overwrite"},
    "group_benefits.certificate_coverage_detail": {"kind": "business", "sources": ["group_benefits.certificate_clean"], "keys": ["certificate_number", "coverage_type_code"], "write_mode": "overwrite"},
    "freedom55.advisor_feed_clean": {"kind": "business", "sources": ["freedom55.advisor_assignments"], "keys": ["advisor_id", "assignment_id"], "write_mode": "overwrite"},
    "investments.climl_clean": {"kind": "business", "sources": ["climl.seg_fund_contracts"], "keys": ["contract_number", "fund_code"], "write_mode": "overwrite"},
    "investments.fund_allocation_detail": {"kind": "business", "sources": ["investments.climl_clean"], "keys": ["contract_number", "fund_code"], "write_mode": "overwrite"},
    "group_retirement.member_clean": {"kind": "business", "sources": ["group_retirement.plan_members"], "keys": ["member_id"], "write_mode": "overwrite"},
    "reinsurance.treaty_clean": {"kind": "business", "sources": ["reinsurance.treaty_data"], "keys": ["treaty_id"], "write_mode": "overwrite"},
    "reference.product_code_mapping": {"kind": "reference", "sources": ["policy.individual_life_clean", "investments.climl_clean", "freedom55.advisor_feed_clean"], "keys": ["legacy_code", "source_system"], "write_mode": "overwrite"},
    "reference.status_code_mapping": {"kind": "reference", "sources": ["policy.individual_life_clean", "reinsurance.treaty_clean", "interactions.callcentre_clean"], "keys": ["legacy_code", "source_system"], "write_mode": "overwrite"},
    "reference.rider_codes": {"kind": "reference", "sources": ["policy.individual_life_clean"], "keys": ["rider_code"], "write_mode": "overwrite"},
    "monitoring.schema_drift_log": {"kind": "monitoring", "sources": list(BRONZE_TABLES.keys()), "keys": ["source_name", "detected_at", "drift_type"], "write_mode": "append"},
    "monitoring.dedup_audit_log": {"kind": "monitoring", "sources": ["policy.individual_life_clean"], "keys": ["policy_number", "_source_system", "_ingested_at"], "write_mode": "append"},
    "monitoring.allocation_errors": {"kind": "monitoring", "sources": ["investments.fund_allocation_detail"], "keys": ["contract_number", "run_id"], "write_mode": "append"},
}

TARGET_ORDER = [
    "customer.master",
    "policy.individual_life_clean",
    "policy.disability_ci_clean",
    "digital.portal_clean",
    "interactions.callcentre_clean",
    "group_benefits.plan_clean",
    "group_benefits.certificate_clean",
    "freedom55.advisor_feed_clean",
    "investments.climl_clean",
    "group_retirement.member_clean",
    "reinsurance.treaty_clean",
    "reference.product_code_mapping",
    "reference.status_code_mapping",
    "reference.rider_codes",
    "policy.individual_life_enriched",
    "policy.policy_rider_detail",
    "investments.fund_allocation_detail",
    "group_benefits.certificate_coverage_detail",
    "monitoring.schema_drift_log",
    "monitoring.dedup_audit_log",
    "monitoring.allocation_errors",
]

PROVINCE_MAP_DATA = [
    ("Ontario", "ON"), ("British Columbia", "BC"), ("Alberta", "AB"),
    ("Quebec", "QC"), ("Manitoba", "MB"), ("Saskatchewan", "SK"),
    ("Nova Scotia", "NS"), ("New Brunswick", "NB"),
    ("Newfoundland and Labrador", "NL"), ("Prince Edward Island", "PE"),
    ("Northwest Territories", "NT"), ("Nunavut", "NU"), ("Yukon", "YT"),
]

PROVINCE_MAP_DF = spark.createDataFrame(PROVINCE_MAP_DATA, ["province_raw", "province_code"])
FREQ_MAP_DF = spark.createDataFrame(
    [("M", 12), ("MONTHLY", 12), ("Q", 4), ("QUARTERLY", 4), ("S", 2), ("SEMI_ANNUAL", 2), ("A", 1), ("ANNUAL", 1)],
    ["premium_frequency_code", "freq_multiplier"],
)

DATAFRAME_CACHE = {}
DEDUP_AUDIT_CACHE = None
ALLOCATION_ERROR_CACHE = None


# COMMAND ----------

# DBTITLE 1,Shared helpers
# MAGIC %md
# MAGIC These helper functions are reused across many targets.
# MAGIC
# MAGIC Highlights:
# MAGIC * target and path name resolution
# MAGIC * source filtering and schema inspection
# MAGIC * PII cleaning and normalization
# MAGIC * Delta write and SCD2 helpers
# MAGIC * shared deduplication and union behavior
# MAGIC

# COMMAND ----------

# DBTITLE 1,Utility functions
# ------------------------------------------------------------------------------
# 3. Utility Functions
# ------------------------------------------------------------------------------
def logical_to_physical(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", name.strip()).strip("_").lower()


def bronze_table_fqn(source_name_value: str) -> str:
    return f"{catalog_name}.{bronze_schema}.{BRONZE_TABLES[source_name_value]}"


def silver_table_fqn(target_name_value: str) -> str:
    return f"{catalog_name}.{silver_schema}.{logical_to_physical(target_name_value)}"


def silver_storage_path(target_name_value: str) -> str:
    return f"{silver_base_path}/{catalog_name}/{silver_schema}/{target_name_value.replace('.', '/')}"


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def ensure_silver_schema() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")


def normalise_filter_value(name: str) -> str:
    return logical_to_physical(name)


def resolve_selected_targets() -> list:
    if target_table_name.upper() != "ALL":
        selected = [target_name for target_name in TARGET_ORDER if normalise_filter_value(target_name) == normalise_filter_value(target_table_name)]
        if not selected:
            raise ValueError(f"Target '{target_table_name}' is not configured.")
        return selected
    return TARGET_ORDER


def filter_incremental(df):
    if not run_date:
        return df
    if "processing_date" in df.columns:
        return df.filter(F.to_date(F.col("processing_date")) == F.to_date(F.lit(run_date)))
    if "ingestion_timestamp" in df.columns:
        return df.filter(F.to_date(F.col("ingestion_timestamp")) == F.to_date(F.lit(run_date)))
    return df


def get_column_names(df) -> set:
    return set(df.columns)


def safe_col(df_or_columns, column_name: str, dtype: str = "string"):
    column_names = df_or_columns if isinstance(df_or_columns, set) else set(df_or_columns.columns)
    return F.col(column_name) if column_name in column_names else F.lit(None).cast(dtype)


def first_existing_column(df_or_columns, candidates: list, dtype: str = "string"):
    column_names = df_or_columns if isinstance(df_or_columns, set) else set(df_or_columns.columns)
    for candidate in candidates:
        if candidate in column_names:
            return F.col(candidate)
    return F.lit(None).cast(dtype)


def has_nested_field(schema: T.StructType, path: str) -> bool:
    current_schema = schema
    for index, part in enumerate(path.split(".")):
        field_match = next((field for field in current_schema.fields if field.name == part), None)
        if field_match is None:
            return False
        if index == len(path.split(".")) - 1:
            return True
        next_type = field_match.dataType
        if isinstance(next_type, T.ArrayType):
            next_type = next_type.elementType
        if not isinstance(next_type, T.StructType):
            return False
        current_schema = next_type
    return True


def nested_or_null(df, candidates: list, dtype: str = "string"):
    for candidate in candidates:
        if "." in candidate:
            if has_nested_field(df.schema, candidate):
                return F.col(candidate).cast(dtype)
        elif candidate in df.columns:
            return F.col(candidate).cast(dtype)
    return F.lit(None).cast(dtype)


def build_batch_id_expr(df_or_columns):
    column_names = df_or_columns if isinstance(df_or_columns, set) else set(df_or_columns.columns)
    components = []
    if "source_file_path" in column_names:
        components.append(F.col("source_file_path").cast("string"))
    if "processing_date" in column_names:
        components.append(F.col("processing_date").cast("string"))
    if "ingestion_timestamp" in column_names:
        components.append(F.col("ingestion_timestamp").cast("string"))
    if not components:
        components = [F.lit(run_id)]
    return F.sha2(F.concat_ws("||", *components), 256)


def apply_common_cleaning(df):
    result_df = df
    result_columns = get_column_names(result_df)
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    email_source = F.lower(F.trim(first_existing_column(result_columns, ["email", "email_address"], "string")))
    phone_source = F.trim(first_existing_column(result_columns, ["phone", "phone_number"], "string"))
    postal_source = F.upper(F.trim(first_existing_column(result_columns, ["postal_code", "postal_code_raw"], "string")))
    province_source = F.trim(first_existing_column(result_columns, ["province", "province_name"], "string"))
    sin_source = F.trim(first_existing_column(result_columns, ["sin", "social_insurance_number"], "string"))

    result_df = result_df.withColumn("email_raw", F.when(email_source == "", F.lit(None).cast("string")).otherwise(email_source))
    result_df = result_df.withColumn(
        "email_clean",
        F.when(
            F.col("email_raw").rlike(email_regex),
            F.concat(F.col("email_raw").substr(1, 2), F.lit("***@"), F.split(F.col("email_raw"), "@").getItem(1)),
        ).otherwise(F.lit(None).cast("string")),
    ).withColumn(
        "email_quality_flag",
        F.when(F.col("email_raw").isNull(), F.lit("MISSING")).when(~F.col("email_raw").rlike(email_regex), F.lit("INVALID")).otherwise(F.lit("VALID")),
    )

    result_df = result_df.withColumn("phone_digits_raw", F.regexp_replace(phone_source, r"[^0-9]", ""))
    result_df = result_df.withColumn(
        "phone_digits",
        F.when(
            (F.length(F.col("phone_digits_raw")) == 11) & F.col("phone_digits_raw").startswith("1"),
            F.col("phone_digits_raw").substr(2, 10),
        ).otherwise(F.col("phone_digits_raw")),
    )
    result_df = result_df.withColumn(
        "phone_standardized",
        F.when(
            (F.length(F.col("phone_digits")) == 10) & F.col("phone_digits").rlike(r"^[2-9]\d{2}[2-9]\d{6}$"),
            F.concat(
                F.col("phone_digits").substr(1, 3),
                F.lit("-"),
                F.col("phone_digits").substr(4, 3),
                F.lit("-"),
                F.col("phone_digits").substr(7, 4),
            ),
        ).otherwise(F.lit(None).cast("string")),
    )
    result_df = result_df.withColumn("phone_valid_flag", F.col("phone_standardized").isNotNull())
    result_df = result_df.withColumn(
        "phone_clean",
        F.when(F.col("phone_standardized").isNotNull(), F.concat(F.lit("***-***-"), F.col("phone_digits").substr(7, 4))).otherwise(F.lit(None).cast("string")),
    )

    result_df = result_df.withColumn("province_raw_input", F.when(province_source == "", F.lit(None).cast("string")).otherwise(province_source))
    result_df = result_df.join(
        F.broadcast(PROVINCE_MAP_DF),
        F.upper(F.trim(F.col("province_raw_input"))) == F.upper(F.trim(F.col("province_raw"))),
        "left",
    ).withColumn(
        "province_clean",
        F.coalesce(F.col("province_code"), F.when(F.length(F.trim(F.col("province_raw_input"))) == 2, F.upper(F.trim(F.col("province_raw_input"))))),
    ).drop("province_raw", "province_code")

    result_df = result_df.withColumn("postal_code_raw", F.when(postal_source == "", F.lit(None).cast("string")).otherwise(postal_source))
    result_df = result_df.withColumn("postal_code_clean", F.when(F.col("postal_code_raw").isNotNull(), F.regexp_replace(F.col("postal_code_raw"), r"\s+", "")))
    result_df = result_df.withColumn("postal_code_valid_flag", F.col("postal_code_clean").rlike(r"^[A-Z]\d[A-Z]\d[A-Z]\d$"))
    result_df = result_df.withColumn("sin_hashed", F.when(sin_source.isNotNull(), F.sha2(sin_source.cast("string"), 256)).otherwise(F.lit(None).cast("string")))

    for pii_column in ["sin", "social_insurance_number"]:
        if pii_column in result_df.columns:
            result_df = result_df.drop(pii_column)

    # Drop intermediate scratch columns â only the clean/hashed outputs belong in silver
    result_df = result_df.drop("email_raw", "phone_digits_raw", "phone_digits", "province_raw_input", "postal_code_raw")

    return result_df


def enforce_null_gate(df, column_names: list, threshold: float):
    valid_columns = [column_name for column_name in column_names if column_name in df.columns]
    if not valid_columns:
        return
    total_rows = df.count()
    if total_rows == 0:
        return

    agg_exprs = [F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias(column_name) for column_name in valid_columns]
    null_counts = df.agg(*agg_exprs).collect()[0].asDict()
    failures = []
    for column_name, null_count in null_counts.items():
        null_rate = (null_count or 0) / total_rows
        if null_rate > threshold:
            failures.append(f"{column_name}={null_rate:.2%}")
    if failures:
        raise ValueError(f"[DQ GATE FAILED] Null threshold exceeded: {', '.join(failures)} > {threshold:.2%}")


IDENTITY_REVIEW_QUEUE_SCHEMA = "compliance"
IDENTITY_REVIEW_QUEUE_TABLE_NAME = "identity_resolution_manual_review"
IDENTITY_REVIEW_QUEUE_TABLE_FQN = f"{catalog_name}.{IDENTITY_REVIEW_QUEUE_SCHEMA}.{IDENTITY_REVIEW_QUEUE_TABLE_NAME}"
IDENTITY_REVIEW_QUEUE_PATH = f"{silver_base_path}/{catalog_name}/{IDENTITY_REVIEW_QUEUE_SCHEMA}/{IDENTITY_REVIEW_QUEUE_TABLE_NAME}"
IDENTITY_AUTO_MERGE_CACHE = None
IDENTITY_REVIEW_QUEUE_CACHE = None


def ensure_schema_exists(schema_name_value: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name_value}")


def _jaro_similarity(left_value, right_value):
    left_string = (left_value or "").strip().upper()
    right_string = (right_value or "").strip().upper()
    if not left_string or not right_string:
        return 0.0
    if left_string == right_string:
        return 1.0

    left_len = len(left_string)
    right_len = len(right_string)
    match_distance = max(left_len, right_len) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    left_matches = [False] * left_len
    right_matches = [False] * right_len
    matches = 0

    for left_index in range(left_len):
        start_index = max(0, left_index - match_distance)
        end_index = min(left_index + match_distance + 1, right_len)
        for right_index in range(start_index, end_index):
            if right_matches[right_index] or left_string[left_index] != right_string[right_index]:
                continue
            left_matches[left_index] = True
            right_matches[right_index] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    transpositions = 0
    right_position = 0
    for left_index in range(left_len):
        if not left_matches[left_index]:
            continue
        while not right_matches[right_position]:
            right_position += 1
        if left_string[left_index] != right_string[right_position]:
            transpositions += 1
        right_position += 1

    return ((matches / left_len) + (matches / right_len) + ((matches - transpositions / 2.0) / matches)) / 3.0


def _jaro_winkler_similarity(left_value, right_value):
    base_score = _jaro_similarity(left_value, right_value)
    left_string = (left_value or "").strip().upper()
    right_string = (right_value or "").strip().upper()
    prefix_length = 0
    for left_character, right_character in zip(left_string[:4], right_string[:4]):
        if left_character != right_character:
            break
        prefix_length += 1
    return float(base_score + (prefix_length * 0.1 * (1.0 - base_score)))


JARO_WINKLER_UDF = F.udf(_jaro_winkler_similarity, T.DoubleType())


def build_identity_resolution_artifacts(policy_df):
    global IDENTITY_AUTO_MERGE_CACHE, IDENTITY_REVIEW_QUEUE_CACHE

    if IDENTITY_AUTO_MERGE_CACHE is not None and IDENTITY_REVIEW_QUEUE_CACHE is not None:
        return IDENTITY_AUTO_MERGE_CACHE, IDENTITY_REVIEW_QUEUE_CACHE

    candidate_source_df = deduplicate_by_window(
        policy_df.select(
            "customer_id",
            "policy_number",
            "date_of_birth",
            "postal_code_clean",
            "province_clean",
            "email_clean",
            "phone_clean",
            "product_type_code",
            "policy_status_code",
            "beneficiary_id",
            "_source_system",
            "_ingested_at",
        ).filter(
            F.col("customer_id").isNotNull()
            & F.col("date_of_birth").isNotNull()
            & F.col("postal_code_clean").isNotNull()
        ),
        ["customer_id", "_source_system", "postal_code_clean", "date_of_birth"],
        ["_ingested_at"],
    )

    candidate_rows = [row.asDict(recursive=True) for row in candidate_source_df.collect()]
    blocked_groups = {}
    for candidate_row in candidate_rows:
        block_key = (candidate_row.get("postal_code_clean"), candidate_row.get("date_of_birth"))
        blocked_groups.setdefault(block_key, []).append(candidate_row)

    auto_merge_records = []
    review_records = []
    review_run_date = F.to_date(F.lit(run_date)) if run_date else None

    for blocked_rows in blocked_groups.values():
        if len(blocked_rows) < 2:
            continue
        for left_index in range(len(blocked_rows)):
            for right_index in range(left_index + 1, len(blocked_rows)):
                left_row = blocked_rows[left_index]
                right_row = blocked_rows[right_index]
                if left_row.get("customer_id") == right_row.get("customer_id"):
                    continue
                if left_row.get("_source_system") == right_row.get("_source_system"):
                    continue

                left_customer_id = left_row.get("customer_id")
                right_customer_id = right_row.get("customer_id")
                if left_customer_id > right_customer_id:
                    left_row, right_row = right_row, left_row
                    left_customer_id, right_customer_id = right_customer_id, left_customer_id

                product_similarity_score = _jaro_winkler_similarity(left_row.get("product_type_code"), right_row.get("product_type_code"))
                status_similarity_score = _jaro_winkler_similarity(left_row.get("policy_status_code"), right_row.get("policy_status_code"))
                beneficiary_similarity_score = _jaro_winkler_similarity(left_row.get("beneficiary_id"), right_row.get("beneficiary_id"))
                email_score = 0.18 if left_row.get("email_clean") and right_row.get("email_clean") and left_row.get("email_clean") == right_row.get("email_clean") else 0.0
                phone_score = 0.12 if left_row.get("phone_clean") and right_row.get("phone_clean") and left_row.get("phone_clean") == right_row.get("phone_clean") else 0.0
                province_score = 0.05 if left_row.get("province_clean") and left_row.get("province_clean") == right_row.get("province_clean") else 0.0
                match_confidence = round(
                    0.55
                    + email_score
                    + phone_score
                    + province_score
                    + (product_similarity_score * 0.05)
                    + (status_similarity_score * 0.03)
                    + (beneficiary_similarity_score * 0.02),
                    4,
                )

                if match_confidence >= 0.85:
                    master_customer_id = min(left_customer_id, right_customer_id)
                    auto_merge_records.append((left_customer_id, master_customer_id, match_confidence))
                    auto_merge_records.append((right_customer_id, master_customer_id, match_confidence))
                elif match_confidence >= 0.70:
                    review_records.append((
                        run_id,
                        left_customer_id,
                        right_customer_id,
                        left_row.get("policy_number"),
                        right_row.get("policy_number"),
                        left_row.get("_source_system"),
                        right_row.get("_source_system"),
                        left_row.get("postal_code_clean"),
                        left_row.get("date_of_birth"),
                        match_confidence,
                        round(product_similarity_score, 4),
                        round(status_similarity_score, 4),
                        round(beneficiary_similarity_score, 4),
                        "PENDING",
                        "Blocked by postal_code_clean + date_of_birth with medium-confidence Jaro-Winkler composite score",
                    ))

    auto_merge_schema = T.StructType([
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("master_customer_id", T.StringType(), True),
        T.StructField("identity_match_confidence", T.DoubleType(), True),
    ])
    if auto_merge_records:
        auto_merge_df = spark.createDataFrame(auto_merge_records, auto_merge_schema).groupBy("customer_id").agg(
            F.min("master_customer_id").alias("master_customer_id"),
            F.max("identity_match_confidence").alias("identity_match_confidence"),
        )
    else:
        auto_merge_df = spark.createDataFrame([], auto_merge_schema)

    review_queue_schema = T.StructType([
        T.StructField("run_id", T.StringType(), True),
        T.StructField("queued_at", T.TimestampType(), True),
        T.StructField("left_customer_id", T.StringType(), True),
        T.StructField("right_customer_id", T.StringType(), True),
        T.StructField("left_policy_number", T.StringType(), True),
        T.StructField("right_policy_number", T.StringType(), True),
        T.StructField("left_source_system", T.StringType(), True),
        T.StructField("right_source_system", T.StringType(), True),
        T.StructField("postal_code_clean", T.StringType(), True),
        T.StructField("date_of_birth", T.DateType(), True),
        T.StructField("match_confidence", T.DoubleType(), True),
        T.StructField("product_similarity_score", T.DoubleType(), True),
        T.StructField("status_similarity_score", T.DoubleType(), True),
        T.StructField("beneficiary_similarity_score", T.DoubleType(), True),
        T.StructField("review_status", T.StringType(), True),
        T.StructField("review_reason", T.StringType(), True),
    ])
    if review_records:
        review_queue_df = spark.createDataFrame(review_records, review_queue_schema).withColumn("queued_at", F.current_timestamp())
    else:
        review_queue_df = spark.createDataFrame([], review_queue_schema).withColumn("queued_at", F.current_timestamp())

    IDENTITY_AUTO_MERGE_CACHE = auto_merge_df
    IDENTITY_REVIEW_QUEUE_CACHE = review_queue_df
    return IDENTITY_AUTO_MERGE_CACHE, IDENTITY_REVIEW_QUEUE_CACHE


def write_identity_resolution_review_queue(review_queue_df):
    ensure_schema_exists(f"{catalog_name}.{IDENTITY_REVIEW_QUEUE_SCHEMA}")
    review_queue_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(IDENTITY_REVIEW_QUEUE_PATH)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {IDENTITY_REVIEW_QUEUE_TABLE_FQN} USING DELTA LOCATION '{IDENTITY_REVIEW_QUEUE_PATH}'"
    )


def persist_identity_resolution_artifacts(policy_df):
    auto_merge_df, review_queue_df = build_identity_resolution_artifacts(policy_df)
    if execution_mode == "RUN":
        write_identity_resolution_review_queue(review_queue_df)
    return auto_merge_df, review_queue_df


def ensure_external_table(target_name_value: str):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {silver_table_fqn(target_name_value)} USING DELTA LOCATION '{silver_storage_path(target_name_value)}'"
    )


def write_delta(df, target_name_value: str, mode: str):
    path = silver_storage_path(target_name_value)
    projected_columns = []
    seen_columns = set()
    for column_name in df.columns:
        if column_name not in seen_columns:
            projected_columns.append(F.col(column_name).alias(column_name))
            seen_columns.add(column_name)
    output_df = df.select(*projected_columns)
    if mode == "append":
        output_df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
    else:
        output_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    ensure_external_table(target_name_value)


def maybe_optimize(target_name_value: str, zorder_columns: list):
    if not optimize_output or execution_mode != "RUN":
        return
    valid_columns = [column_name for column_name in zorder_columns if column_name]
    table_name = silver_table_fqn(target_name_value)
    if valid_columns:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({', '.join(valid_columns)})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")


def apply_scd2(df, target_name_value: str, natural_key: str):
    target_table = silver_table_fqn(target_name_value)
    target_path = silver_storage_path(target_name_value)
    today_expr = F.to_date(F.lit(run_date)) if run_date else F.current_date()

    tracked_columns = [column_name for column_name in SCD2_TRACKED_COLUMNS if column_name in df.columns]
    if not tracked_columns:
        tracked_columns = [column_name for column_name in df.columns if column_name not in {natural_key, "_ingested_at", "_batch_id"}]

    source_df = (
        df.drop("effective_date", "expiry_date", "is_current", "_updated_at")
        .withColumn("effective_date", today_expr)
        .withColumn("expiry_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_updated_at", F.current_timestamp())
    )

    if not table_exists(target_table):
        source_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        ensure_external_table(target_name_value)
        return source_df

    current_active_df = spark.table(target_table).filter(F.col("is_current") == True)
    comparison_df = source_df.alias("source").join(
        current_active_df.alias("target"),
        F.col(f"source.{natural_key}") == F.col(f"target.{natural_key}"),
        "left",
    )

    change_condition = None
    for column_name in tracked_columns:
        if column_name in current_active_df.columns:
            column_change = ~F.col(f"source.{column_name}").eqNullSafe(F.col(f"target.{column_name}"))
            change_condition = column_change if change_condition is None else (change_condition | column_change)
    if change_condition is None:
        change_condition = F.lit(False)

    changed_keys_df = comparison_df.filter(
        F.col(f"target.{natural_key}").isNotNull() & change_condition
    ).select(F.col(f"source.{natural_key}").alias(natural_key)).dropDuplicates([natural_key])

    new_or_changed_df = comparison_df.filter(
        F.col(f"target.{natural_key}").isNull() | change_condition
    ).select("source.*")

    if changed_keys_df.limit(1).count() > 0:
        expiry_updates_df = changed_keys_df.withColumn("new_expiry_date", F.date_sub(today_expr, 1)).withColumn("updated_at", F.current_timestamp())
        DeltaTable.forPath(spark, target_path).alias("target").merge(
            expiry_updates_df.alias("updates"),
            f"target.{natural_key} = updates.{natural_key} AND target.is_current = true",
        ).whenMatchedUpdate(set={
            "is_current": "false",
            "expiry_date": "updates.new_expiry_date",
            "_updated_at": "updates.updated_at",
        }).execute()

    if new_or_changed_df.limit(1).count() > 0:
        new_or_changed_df.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)

    ensure_external_table(target_name_value)
    return spark.table(target_table)


def deduplicate_by_window(df, keys: list, order_columns: list):
    valid_keys = [column_name for column_name in keys if column_name in df.columns]
    valid_order = [column_name for column_name in order_columns if column_name in df.columns]
    if not valid_keys:
        return df
    if valid_order:
        window_spec = Window.partitionBy(*valid_keys).orderBy(*[F.col(column_name).desc_nulls_last() for column_name in valid_order])
        return df.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1).drop("_row_num")
    return df.dropDuplicates(valid_keys)


def union_all(dataframes: list):
    if not dataframes:
        raise ValueError("No dataframes to union")
    return reduce(lambda left_df, right_df: left_df.unionByName(right_df, allowMissingColumns=True), dataframes)


def read_bronze_source(source_name_value: str):
    df = spark.table(bronze_table_fqn(source_name_value))
    return filter_incremental(df)


def schema_signature(source_name_value: str):
    source_df = spark.table(bronze_table_fqn(source_name_value))
    columns = get_column_names(source_df)
    expected = SOURCE_EXPECTED_COLUMNS[source_name_value]
    unexpected = sorted(columns - expected)
    missing = sorted(expected - columns)
    return columns, unexpected, missing


# COMMAND ----------

# DBTITLE 1,Source normalization
# MAGIC %md
# MAGIC Each helper in this section reads one bronze source and standardizes it for silver processing.
# MAGIC
# MAGIC Patterns used:
# MAGIC * source-specific column mapping
# MAGIC * safe column selection for evolving schemas
# MAGIC * consistent `_ingested_at`, `_source_system`, and `_batch_id`
# MAGIC * Adobe-only JSON flattening for digital events
# MAGIC

# COMMAND ----------

# DBTITLE 1,Source normalization helpers
# ------------------------------------------------------------------------------
# 4. Source Normalization Helpers
# ------------------------------------------------------------------------------
def normalize_salesforce_for_customer():
    df = read_bronze_source("salesforce.crm")
    result_df = df.select(
        safe_col(df, "customer_id").alias("customer_id"),
        safe_col(df, "first_name").alias("first_name"),
        safe_col(df, "last_name").alias("last_name"),
        safe_col(df, "email").alias("email"),
        safe_col(df, "phone").alias("phone"),
        safe_col(df, "province").alias("province"),
        safe_col(df, "postal_code").alias("postal_code"),
        safe_col(df, "channel").alias("channel"),
        safe_col(df, "advisor_id").alias("advisor_id"),
        safe_col(df, "created_date").cast("timestamp").alias("created_date"),
        safe_col(df, "updated_date").cast("timestamp").alias("updated_date"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        safe_col(df, "source_system").alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return apply_common_cleaning(result_df)


def build_customer_contact_lookup():
    salesforce_df = normalize_salesforce_for_customer().select(
        "customer_id",
        F.col("email_clean").alias("contact_email_clean"),
        F.col("email_quality_flag").alias("contact_email_quality_flag"),
        F.col("phone_clean").alias("contact_phone_clean"),
        F.col("province_clean").alias("contact_province_clean"),
        F.col("postal_code_clean").alias("contact_postal_code_clean"),
        F.col("postal_code_valid_flag").alias("contact_postal_code_valid_flag"),
    )
    return deduplicate_by_window(salesforce_df, ["customer_id"], [])


def normalize_gwl_policy():
    df = read_bronze_source("gwl_policy.individual_life")
    gwl_policy_id = F.coalesce(
        safe_col(df, "gwl_policy_id"),
        safe_col(df, "policy_id"),
        F.sha2(F.concat_ws("||", *[F.col(column_name).cast("string") for column_name in df.columns]), 256),
    )
    result_df = df.select(
        gwl_policy_id.alias("policy_number"),
        gwl_policy_id.alias("legacy_policy_number"),
        safe_col(df, "customer_id").alias("customer_id"),
        safe_col(df, "product").alias("product_type_code"),
        safe_col(df, "sum_assured").cast(T.DecimalType(15, 2)).alias("face_amount"),
        safe_col(df, "premium").cast(T.DecimalType(12, 2)).alias("premium_amount"),
        F.upper(safe_col(df, "frequency")).alias("premium_frequency_code"),
        F.to_date(F.concat_ws("-", safe_col(df, "issue_year"), F.lit("01"), F.lit("01")), "yyyy-MM-dd").alias("issue_date"),
        safe_col(df, "expiry_date").cast("date").alias("expiry_date"),
        safe_col(df, "status").alias("policy_status_code"),
        F.lit(None).cast("string").alias("beneficiary_id"),
        F.lit(None).cast("string").alias("rider_codes"),
        F.lit(None).cast("string").alias("underwriting_class_code"),
        safe_col(df, "province").alias("province"),
        safe_col(df, "postal_code").alias("postal_code"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        F.coalesce(safe_col(df, "source_system"), F.lit("gwl_policy_admin")).alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return apply_common_cleaning(result_df)


def normalize_ll_policy():
    df = read_bronze_source("ll_policy.individual_life")
    ll_policy_id = F.coalesce(
        safe_col(df, "policy_id"),
        F.sha2(F.concat_ws("||", *[F.col(column_name).cast("string") for column_name in df.columns]), 256),
    )
    result_df = df.select(
        F.concat(F.lit("LL-"), ll_policy_id).alias("policy_number"),
        ll_policy_id.alias("legacy_policy_number"),
        safe_col(df, "customer_id").alias("customer_id"),
        safe_col(df, "product_code").alias("product_type_code"),
        safe_col(df, "face_amount").cast(T.DecimalType(15, 2)).alias("face_amount"),
        safe_col(df, "premium").cast(T.DecimalType(12, 2)).alias("premium_amount"),
        F.upper(safe_col(df, "frequency")).alias("premium_frequency_code"),
        safe_col(df, "issue_date").cast("date").alias("issue_date"),
        safe_col(df, "expiry_date").cast("date").alias("expiry_date"),
        safe_col(df, "status").alias("policy_status_code"),
        safe_col(df, "beneficiary").alias("beneficiary_id"),
        safe_col(df, "rider_codes").alias("rider_codes"),
        safe_col(df, "underwriter").alias("underwriting_class_code"),
        safe_col(df, "province").alias("province"),
        safe_col(df, "postal_code").alias("postal_code"),
        safe_col(df, "sin").alias("sin"),
        safe_col(df, "dob").cast("date").alias("date_of_birth"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        F.coalesce(safe_col(df, "source_system"), F.lit("ll_policy_admin")).alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return apply_common_cleaning(result_df)


def normalize_portal_events():
    df = read_bronze_source("portal.digital_events")
    result_df = df.select(
        F.sha2(F.concat_ws("||", safe_col(df, "claim_id"), safe_col(df, "policy_id"), safe_col(df, "customer_id"), safe_col(df, "processing_date")), 256).alias("event_id"),
        safe_col(df, "customer_id").alias("customer_id"),
        safe_col(df, "policy_id").alias("policy_number"),
        safe_col(df, "claim_type").alias("event_type"),
        F.coalesce(safe_col(df, "reported_date").cast("timestamp"), safe_col(df, "claim_date").cast("timestamp"), safe_col(df, "ingestion_timestamp").cast("timestamp")).alias("event_timestamp"),
        safe_col(df, "status").alias("event_status"),
        safe_col(df, "notes").alias("event_notes"),
        safe_col(df, "source_system").alias("_source_system"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return result_df


def normalize_callcentre_interactions():
    df = read_bronze_source("call_centre.interactions")
    result_df = df.select(
        safe_col(df, "interaction_id").alias("interaction_id"),
        safe_col(df, "customer_id").alias("customer_id"),
        safe_col(df, "agent_id").alias("agent_id"),
        safe_col(df, "call_start_ts").cast("timestamp").alias("call_start_ts"),
        safe_col(df, "call_end_ts").cast("timestamp").alias("call_end_ts"),
        safe_col(df, "channel").alias("channel"),
        safe_col(df, "issue_type").alias("issue_type"),
        safe_col(df, "resolution_status").alias("interaction_status"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        F.coalesce(safe_col(df, "source_system"), F.lit("avaya_call_centre")).alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    result_df = result_df.withColumn(
        "call_duration_minutes",
        F.when(
            F.col("call_end_ts").isNotNull() & F.col("call_start_ts").isNotNull(),
            (F.col("call_end_ts").cast("long") - F.col("call_start_ts").cast("long")) / 60.0,
        ).cast("double"),
    )
    return result_df


def normalize_group_benefits_base():
    df = read_bronze_source("group_benefits.plan_members")
    coverage_fragments = []
    if "coverage_type" in df.columns:
        coverage_fragments.append(F.col("coverage_type"))
    for column_name, value_name in [("dental", "GRP_DENTAL"), ("vision", "GRP_VISION"), ("ltd", "GRP_LTD")]:
        if column_name in df.columns:
            coverage_fragments.append(F.when(F.upper(F.col(column_name).cast("string")).isin("Y", "YES", "TRUE", "1"), F.lit(value_name)))
    coverage_array = F.array(*coverage_fragments) if coverage_fragments else F.array(F.lit(None))

    result_df = df.select(
        safe_col(df, "member_id").alias("member_id"),
        safe_col(df, "plan_id").alias("plan_id"),
        F.concat(F.lit("CERT-"), safe_col(df, "member_id")).alias("certificate_number"),
        safe_col(df, "employer").alias("employer_name"),
        safe_col(df, "first_name").alias("first_name"),
        safe_col(df, "last_name").alias("last_name"),
        safe_col(df, "dob").cast("date").alias("date_of_birth"),
        safe_col(df, "province").alias("province"),
        safe_col(df, "effective_date").cast("date").alias("effective_date"),
        safe_col(df, "termination_date").cast("date").alias("termination_date"),
        F.array_join(F.array_distinct(F.filter(coverage_array, lambda x: x.isNotNull())), ",").alias("coverage_type_codes_enrolled"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        safe_col(df, "source_system").alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return apply_common_cleaning(result_df)


def normalize_freedom55_assignments():
    df = read_bronze_source("freedom55.advisor_assignments")
    result_df = df.select(
        safe_col(df, "assignment_id").alias("assignment_id"),
        safe_col(df, "advisor_id").alias("advisor_id"),
        safe_col(df, "customer_id").alias("customer_id"),
        safe_col(df, "assignment_start_date").cast("date").alias("assignment_start_date"),
        safe_col(df, "assignment_end_date").cast("date").alias("assignment_end_date"),
        safe_col(df, "region").alias("region"),
        safe_col(df, "channel").alias("channel"),
        safe_col(df, "status").alias("advisor_assignment_status"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        F.coalesce(safe_col(df, "source_system"), F.lit("f55_advisor")).alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
        F.lit(None).cast("string").alias("policy_number"),
        F.lit(None).cast("string").alias("product_type_code"),
    )
    return result_df


def normalize_climl_contracts():
    df = read_bronze_source("climl.seg_fund_contracts")
    result_df = df.select(
        safe_col(df, "account_id").alias("contract_number"),
        safe_col(df, "customer_id").alias("customer_id"),
        safe_col(df, "asset_type").alias("product_type_code"),
        safe_col(df, "fund_code").alias("fund_code"),
        safe_col(df, "units").cast("double").alias("units"),
        safe_col(df, "nav").cast("double").alias("nav"),
        safe_col(df, "market_value").cast("double").alias("market_value"),
        safe_col(df, "purchase_date").cast("date").alias("purchase_date"),
        safe_col(df, "currency").alias("currency"),
        safe_col(df, "benchmark").alias("benchmark"),
        safe_col(df, "ytd_return_pct").cast("double").alias("ytd_return_pct"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        safe_col(df, "source_system").alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return result_df


def normalize_group_retirement_members():
    df = read_bronze_source("group_retirement.plan_members")
    result_df = df.select(
        safe_col(df, "member_id").alias("member_id"),
        safe_col(df, "plan_id").alias("plan_id"),
        safe_col(df, "employer_id").alias("employer_id"),
        safe_col(df, "retirement_date").cast("date").alias("retirement_date"),
        safe_col(df, "contribution_amount").cast(T.DecimalType(15, 2)).alias("contribution_amount"),
        safe_col(df, "vesting_status").alias("vesting_status"),
        safe_col(df, "member_status").alias("member_status"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        F.coalesce(safe_col(df, "source_system"), F.lit("group_retirement")).alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return result_df


def normalize_reinsurance_treaties():
    df = read_bronze_source("reinsurance.treaty_data")
    result_df = df.select(
        safe_col(df, "treaty_id").alias("treaty_id"),
        safe_col(df, "policy_id").alias("policy_number"),
        safe_col(df, "reinsurer").alias("reinsurer_name"),
        safe_col(df, "ceded_amount").cast(T.DecimalType(15, 2)).alias("ceded_amount"),
        safe_col(df, "retained_amount").cast(T.DecimalType(15, 2)).alias("retained_amount"),
        safe_col(df, "premium_ceded").cast(T.DecimalType(15, 2)).alias("premium_ceded"),
        safe_col(df, "claim_recovered").cast(T.DecimalType(15, 2)).alias("claim_recovered"),
        safe_col(df, "effective_date").cast("date").alias("effective_date"),
        safe_col(df, "expiry_date").cast("date").alias("expiry_date"),
        safe_col(df, "product").alias("product_type_code"),
        safe_col(df, "status").alias("policy_status_code"),
        safe_col(df, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        safe_col(df, "source_system").alias("_source_system"),
        build_batch_id_expr(df).alias("_batch_id"),
    )
    return result_df


def read_adobe_json_events():
    bronze_df = read_bronze_source("adobe_analytics.digital_events")
    path_rows = bronze_df.select("source_file_path").where(F.col("source_file_path").isNotNull()).distinct().collect()
    json_paths = [row[0] for row in path_rows if row[0]]
    if not json_paths:
        schema = T.StructType([
            T.StructField("event_id", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("policy_number", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("event_timestamp", T.TimestampType(), True),
            T.StructField("journey_name", T.StringType(), True),
            T.StructField("page_name", T.StringType(), True),
            T.StructField("raw_event_json", T.StringType(), True),
            T.StructField("_source_system", T.StringType(), True),
            T.StructField("_ingested_at", T.TimestampType(), True),
            T.StructField("_batch_id", T.StringType(), True),
        ])
        return spark.createDataFrame([], schema)

    raw_json_df = spark.read.option("multiLine", True).json(json_paths)
    flattened_df = raw_json_df
    for candidate_array in ["events", "data", "rows", "items", "records"]:
        flattened_columns = get_column_names(flattened_df)
        if candidate_array in flattened_columns:
            flattened_df = flattened_df.withColumn(candidate_array, F.explode_outer(F.col(candidate_array)))
            candidate_dtype = flattened_df.schema[candidate_array].dataType
            if isinstance(candidate_dtype, T.StructType):
                inner_field_names = candidate_dtype.names
                inner_fields = [F.col(f"{candidate_array}.{field_name}").alias(field_name) for field_name in inner_field_names]
                passthrough_fields = [F.col(column_name) for column_name in flattened_df.columns if column_name != candidate_array]
                flattened_df = flattened_df.select(*passthrough_fields, *inner_fields)
            break

    raw_event_struct = F.struct(*[F.col(column_name) for column_name in flattened_df.columns])

    result_df = flattened_df.select(
        F.coalesce(
            nested_or_null(flattened_df, ["event_id", "eventId", "id", "hitid_high", "visit_num"], "string"),
            F.sha2(F.to_json(raw_event_struct), 256),
        ).alias("event_id"),
        nested_or_null(flattened_df, ["customer_id", "customerId", "identity.customer_id", "identity.customerId", "cust_id"], "string").alias("customer_id"),
        nested_or_null(flattened_df, ["policy_number", "policy_id", "policyId", "policy.id"], "string").alias("policy_number"),
        nested_or_null(flattened_df, ["event_type", "eventType", "type", "page_event"], "string").alias("event_type"),
        F.coalesce(
            nested_or_null(flattened_df, ["event_timestamp", "timestamp", "ts", "occurred_at", "api_metadata.extracted_at"], "timestamp"),
            F.current_timestamp(),
        ).alias("event_timestamp"),
        nested_or_null(flattened_df, ["journey_name", "journeyName", "journey.name"], "string").alias("journey_name"),
        nested_or_null(flattened_df, ["page_name", "pageName", "page.name", "page_url"], "string").alias("page_name"),
        F.to_json(raw_event_struct).alias("raw_event_json"),
        F.lit("adobe_analytics.digital_events").alias("_source_system"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run_id).alias("_batch_id"),
    )
    return result_df


# COMMAND ----------

# DBTITLE 1,Target builders and orchestration
# MAGIC %md
# MAGIC This section assembles normalized sources into the final silver outputs.
# MAGIC
# MAGIC It includes:
# MAGIC * business target builders
# MAGIC * reference and monitoring builders
# MAGIC * a target dispatch map
# MAGIC * the main execution loop for PLAN, TEST, and RUN
# MAGIC

# COMMAND ----------

# DBTITLE 1,Target builders and execution
# ------------------------------------------------------------------------------
# 5. Target Builders
# ------------------------------------------------------------------------------
def build_customer_master():
    contact_df = normalize_salesforce_for_customer().select(
        "customer_id", "first_name", "last_name", "email_clean", "email_quality_flag", "phone_clean", "province_clean", "postal_code_clean", "postal_code_valid_flag", "channel", "advisor_id", "_ingested_at", "_source_system", "_batch_id"
    )
    gwl_df = normalize_gwl_policy().select(
        "customer_id", "product_type_code", "province_clean", "postal_code_clean", "postal_code_valid_flag", "_ingested_at", "_source_system", "_batch_id"
    )
    sap_source_df = read_bronze_source("sap_billing.invoices")
    sap_source_columns = get_column_names(sap_source_df)
    sap_df = sap_source_df.select(
        safe_col(sap_source_columns, "customer_id").alias("customer_id"),
        safe_col(sap_source_columns, "policy_id").alias("policy_number"),
        safe_col(sap_source_columns, "status").alias("billing_status"),
        safe_col(sap_source_columns, "amount").cast(T.DecimalType(12, 2)).alias("billing_amount"),
        safe_col(sap_source_columns, "ingestion_timestamp").cast("timestamp").alias("_ingested_at"),
        safe_col(sap_source_columns, "source_system").alias("_source_system"),
        build_batch_id_expr(sap_source_columns).alias("_batch_id"),
    )
    combined_df = union_all([contact_df, gwl_df, sap_df])
    policy_identity_df = build_target_dataframe("policy.individual_life_clean").select("customer_id", "master_customer_id").dropDuplicates(["customer_id"])
    mastered_df = combined_df.join(F.broadcast(policy_identity_df), ["customer_id"], "left").withColumn(
        "master_customer_id", F.coalesce(F.col("master_customer_id"), F.col("customer_id"))
    )
    master_window = Window.partitionBy("master_customer_id").orderBy(
        F.col("email_clean").isNotNull().desc(),
        F.col("phone_clean").isNotNull().desc(),
        F.col("advisor_id").isNotNull().desc(),
        F.col("_ingested_at").desc_nulls_last(),
    )
    return mastered_df.withColumn("_row_num", F.row_number().over(master_window)).filter(F.col("_row_num") == 1).drop("_row_num").withColumn(
        "source_customer_id", F.col("customer_id")
    ).withColumn(
        "customer_id", F.col("master_customer_id")
    ).drop("master_customer_id")


def build_policy_individual_life_clean():
    global DEDUP_AUDIT_CACHE

    customer_lookup = build_customer_contact_lookup()
    ll_df = normalize_ll_policy()
    gwl_df = normalize_gwl_policy()
    combined_df = union_all([ll_df, gwl_df]).join(customer_lookup, ["customer_id"], "left")

    policy_df = combined_df.withColumns({
        "email_clean": F.coalesce(F.col("email_clean"), F.col("contact_email_clean")),
        "email_quality_flag": F.coalesce(F.col("email_quality_flag"), F.col("contact_email_quality_flag")),
        "phone_clean": F.coalesce(F.col("phone_clean"), F.col("contact_phone_clean")),
        "province_clean": F.coalesce(F.col("province_clean"), F.col("contact_province_clean")),
        "postal_code_clean": F.coalesce(F.col("postal_code_clean"), F.col("contact_postal_code_clean")),
        "postal_code_valid_flag": F.coalesce(F.col("postal_code_valid_flag"), F.col("contact_postal_code_valid_flag")),
        "_ingested_year": F.year(F.col("_ingested_at")),
        "_ingested_month": F.month(F.col("_ingested_at")),
    }).drop(
        "contact_email_clean",
        "contact_email_quality_flag",
        "contact_phone_clean",
        "contact_province_clean",
        "contact_postal_code_clean",
        "contact_postal_code_valid_flag",
    )

    intra_window = Window.partitionBy("policy_number").orderBy(F.col("_ingested_at").desc_nulls_last())
    intra_ranked = policy_df.withColumn("_row_num", F.row_number().over(intra_window))
    intra_dedup_df = intra_ranked.filter(F.col("_row_num") == 1).drop("_row_num")

    cross_window = Window.partitionBy("customer_id", "product_type_code", "issue_date", "face_amount").orderBy(
        F.when(F.col("_source_system").contains("ll"), 1).when(F.col("_source_system").contains("gwl"), 2).otherwise(99),
        F.col("_ingested_at").desc_nulls_last(),
    )
    cross_ranked = intra_dedup_df.withColumn("_dedup_rank", F.row_number().over(cross_window)).withColumn("_is_duplicate", F.col("_dedup_rank") > 1)

    DEDUP_AUDIT_CACHE = cross_ranked.filter(F.col("_is_duplicate") == True).select(
        "policy_number", "customer_id", "_source_system", "_ingested_at", "_batch_id", "_dedup_rank"
    )

    clean_df = cross_ranked.filter(F.col("_is_duplicate") == False).drop("_dedup_rank", "_is_duplicate")
    auto_merge_df, review_queue_df = persist_identity_resolution_artifacts(clean_df)
    review_customer_df = review_queue_df.select(F.col("left_customer_id").alias("customer_id")).unionByName(
        review_queue_df.select(F.col("right_customer_id").alias("customer_id")),
        allowMissingColumns=True,
    ).dropDuplicates(["customer_id"])
    clean_df = clean_df.join(F.broadcast(auto_merge_df), ["customer_id"], "left").join(
        F.broadcast(review_customer_df.withColumn("identity_manual_review_flag", F.lit(True))),
        ["customer_id"],
        "left",
    ).withColumns({
        "master_customer_id": F.coalesce(F.col("master_customer_id"), F.col("customer_id")),
        "identity_match_confidence": F.col("identity_match_confidence"),
        "identity_manual_review_flag": F.coalesce(F.col("identity_manual_review_flag"), F.lit(False)),
        "identity_resolution_status": F.when(F.col("identity_match_confidence").isNotNull(), F.lit("AUTO_MERGED")).when(F.col("identity_manual_review_flag") == True, F.lit("PENDING_MANUAL_REVIEW")).otherwise(F.lit("DISTINCT")),
    })
    if execution_mode == "RUN":
        enforce_null_gate(clean_df, ["policy_number", "_ingested_at", "_source_system"], dq_threshold_pct)
    return clean_df


def build_policy_disability_ci_clean():
    ll_df = normalize_ll_policy()
    filtered_df = ll_df.filter(F.upper(F.coalesce(F.col("product_type_code"), F.lit(""))).rlike("(DI|DISABILITY|CI|CRITICAL)"))
    return deduplicate_by_window(filtered_df, ["policy_number"], ["_ingested_at"])


def canonicalise_product_expr(column_expr):
    normalized = F.upper(F.regexp_replace(F.coalesce(column_expr, F.lit("UNKNOWN")), r"[^A-Z0-9]", ""))
    return (
        F.when(normalized.isin("T20", "TERM20", "LT20"), F.lit("TERM20"))
        .when(normalized.isin("WL", "WHOLELIFE"), F.lit("WHOLELIFE"))
        .when(normalized.isin("UL", "UNIVERSALLIFE"), F.lit("UNIVERSALLIFE"))
        .otherwise(normalized)
    )


def canonicalise_status_expr(column_expr):
    normalized = F.upper(F.regexp_replace(F.coalesce(column_expr, F.lit("UNKNOWN")), r"[^A-Z0-9]", ""))
    return (
        F.when(normalized.isin("01", "ACT", "ACTIVE", "INFORCE"), F.lit("INFORCE"))
        .when(normalized.isin("03", "LAP", "LAPSE", "LAPSED"), F.lit("LAPSED"))
        .when(normalized.isin("GRACE", "GRACEPERIOD"), F.lit("GRACE"))
        .when(normalized.isin("PAIDUP"), F.lit("PAID_UP"))
        .when(normalized.isin("SUSPENDED"), F.lit("SUSPENDED"))
        .when(normalized.isin("PENDING"), F.lit("PENDING"))
        .when(normalized.isin("CANCELLED", "CANCELED"), F.lit("CANCELLED"))
        .otherwise(normalized)
    )


def build_reference_product_code_mapping():
    policy_df = build_target_dataframe("policy.individual_life_clean")
    investments_df = build_target_dataframe("investments.climl_clean")
    advisor_df = build_target_dataframe("freedom55.advisor_feed_clean")

    mapping_df = union_all([
        policy_df.select(F.col("product_type_code").alias("legacy_code"), F.col("_source_system").alias("source_system")),
        investments_df.select(F.col("product_type_code").alias("legacy_code"), F.col("_source_system").alias("source_system")),
        advisor_df.select(F.col("product_type_code").alias("legacy_code"), F.col("_source_system").alias("source_system")),
    ]).filter(F.col("legacy_code").isNotNull()).dropDuplicates(["legacy_code", "source_system"])

    return mapping_df.withColumn("canonical_code", canonicalise_product_expr(F.col("legacy_code"))).withColumn(
        "product_category",
        F.when(F.col("canonical_code").rlike("TERM"), F.lit("TERM_LIFE")).when(F.col("canonical_code").rlike("WHOLE"), F.lit("WHOLE_LIFE")).otherwise(F.lit("UNKNOWN")),
    )


def build_reference_status_code_mapping():
    policy_df = build_target_dataframe("policy.individual_life_clean")
    reinsurance_df = build_target_dataframe("reinsurance.treaty_clean")
    interactions_df = build_target_dataframe("interactions.callcentre_clean")

    mapping_df = union_all([
        policy_df.select(F.col("policy_status_code").alias("legacy_code"), F.col("_source_system").alias("source_system")),
        reinsurance_df.select(F.col("policy_status_code").alias("legacy_code"), F.col("_source_system").alias("source_system")),
        interactions_df.select(F.col("interaction_status").alias("legacy_code"), F.col("_source_system").alias("source_system")),
    ]).filter(F.col("legacy_code").isNotNull()).dropDuplicates(["legacy_code", "source_system"])

    return mapping_df.withColumn("canonical_status_code", canonicalise_status_expr(F.col("legacy_code")))


def build_reference_rider_codes():
    policy_df = build_target_dataframe("policy.individual_life_clean")
    rider_df = policy_df.withColumn("rider_code", F.explode_outer(F.split(F.coalesce(F.col("rider_codes"), F.lit("")), ","))).withColumn("rider_code", F.trim(F.col("rider_code"))).filter(F.col("rider_code") != "")
    return rider_df.select("rider_code").dropDuplicates().withColumn("rider_description", F.lit(None).cast("string")).withColumn("rider_category", F.lit("UNKNOWN"))


def build_policy_individual_life_enriched():
    policy_df = build_target_dataframe("policy.individual_life_clean")
    product_map_df = build_target_dataframe("reference.product_code_mapping")
    status_map_df = build_target_dataframe("reference.status_code_mapping")

    enriched_df = policy_df.join(F.broadcast(FREQ_MAP_DF), ["premium_frequency_code"], "left").withColumn(
        "annualised_premium",
        F.round(F.col("premium_amount") * F.coalesce(F.col("freq_multiplier"), F.lit(1)), 2).cast(T.DecimalType(12, 2)),
    ).drop("freq_multiplier")

    product_map_lookup_df = F.broadcast(product_map_df.select(
        F.col("legacy_code").alias("product_legacy_code"),
        F.col("source_system").alias("product_source_system"),
        F.col("canonical_code"),
        F.col("product_category"),
    ))
    enriched_df = enriched_df.join(
        product_map_lookup_df,
        (enriched_df["product_type_code"] == product_map_lookup_df["product_legacy_code"]) &
        (enriched_df["_source_system"] == product_map_lookup_df["product_source_system"]),
        "left",
    ).withColumns({
        "product_type_code_canonical": F.coalesce(F.col("canonical_code"), enriched_df["product_type_code"]),
        "product_category": F.coalesce(F.col("product_category"), F.lit("UNKNOWN")),
    }).drop("product_legacy_code", "product_source_system", "canonical_code")

    status_map_lookup_df = F.broadcast(status_map_df.select(
        F.col("legacy_code").alias("status_legacy_code"),
        F.col("source_system").alias("status_source_system"),
        F.col("canonical_status_code"),
    ))
    enriched_df = enriched_df.join(
        status_map_lookup_df,
        (enriched_df["policy_status_code"] == status_map_lookup_df["status_legacy_code"]) &
        (enriched_df["_source_system"] == status_map_lookup_df["status_source_system"]),
        "left",
    ).withColumn(
        "policy_status_canonical", F.coalesce(F.col("canonical_status_code"), enriched_df["policy_status_code"])
    ).drop("status_legacy_code", "status_source_system", "canonical_status_code")

    reference_date = F.to_date(F.lit(run_date)) if run_date else F.current_date()
    enriched_df = enriched_df.withColumn("policy_tenure_days", F.datediff(reference_date, F.col("issue_date"))).withColumn(
        "term_expiry_days_remaining",
        F.when(F.col("expiry_date").isNotNull(), F.datediff(F.col("expiry_date"), reference_date)),
    ).withColumn(
        "term_expiring_90d_flag", F.when(F.col("term_expiry_days_remaining").between(0, 90), F.lit(True)).otherwise(F.lit(False))
    ).withColumn(
        "churn_risk_signal",
        F.when(F.col("policy_status_canonical") == "GRACE", F.lit("HIGH")).when(F.col("term_expiring_90d_flag") == True, F.lit("HIGH")).when(F.col("policy_tenure_days") < 365, F.lit("MEDIUM")).otherwise(F.lit("LOW")),
    )

    return enriched_df


def build_policy_rider_detail():
    enriched_df = build_target_dataframe("policy.individual_life_enriched")
    rider_ref_df = build_target_dataframe("reference.rider_codes")
    rider_df = enriched_df.withColumn("rider_code", F.explode_outer(F.split(F.coalesce(F.col("rider_codes"), F.lit("")), ","))).withColumn("rider_code", F.trim(F.col("rider_code"))).filter(F.col("rider_code") != "").drop("rider_codes")
    return rider_df.join(F.broadcast(rider_ref_df), ["rider_code"], "left")


def build_digital_portal_clean():
    adobe_df = read_adobe_json_events()
    portal_df = normalize_portal_events()
    combined_df = union_all([adobe_df, portal_df])
    return deduplicate_by_window(combined_df, ["event_id"], ["event_timestamp", "_ingested_at"])


def build_group_benefits_plan_clean():
    base_df = normalize_group_benefits_base()
    return deduplicate_by_window(base_df, ["plan_id", "member_id"], ["_ingested_at"])


def build_group_benefits_certificate_clean():
    base_df = normalize_group_benefits_base()
    return deduplicate_by_window(base_df, ["certificate_number"], ["_ingested_at"])


def build_group_benefits_certificate_coverage_detail():
    certificate_df = build_target_dataframe("group_benefits.certificate_clean")
    return certificate_df.withColumn("coverage_type_code", F.explode_outer(F.split(F.coalesce(F.col("coverage_type_codes_enrolled"), F.lit("")), ","))).withColumn("coverage_type_code", F.trim(F.col("coverage_type_code"))).filter(F.col("coverage_type_code") != "")


def build_freedom55_advisor_feed_clean():
    return deduplicate_by_window(normalize_freedom55_assignments(), ["advisor_id", "assignment_id"], ["_ingested_at"])


def build_investments_climl_clean():
    return deduplicate_by_window(normalize_climl_contracts(), ["contract_number", "fund_code"], ["_ingested_at"])


def build_investments_fund_allocation_detail():
    global ALLOCATION_ERROR_CACHE

    investments_df = build_target_dataframe("investments.climl_clean")
    if "fund_code" in investments_df.columns and "market_value" in investments_df.columns:
        window_spec = Window.partitionBy("contract_number")
        detail_df = investments_df.withColumn(
            "allocation_pct",
            F.when(F.sum(F.col("market_value")).over(window_spec) != 0, F.col("market_value") / F.sum(F.col("market_value")).over(window_spec)).otherwise(F.lit(None).cast("double")),
        )
    else:
        detail_df = investments_df.withColumn("allocation_pct", F.lit(None).cast("double"))

    alloc_check_df = detail_df.groupBy("contract_number").agg(F.round(F.sum(F.col("allocation_pct")), 4).alias("total_allocation_pct")).filter(F.abs(F.col("total_allocation_pct") - 1.0) > 0.001)
    ALLOCATION_ERROR_CACHE = alloc_check_df.withColumn("run_id", F.lit(run_id)).withColumn("detected_at", F.current_timestamp())
    return detail_df


def build_group_retirement_member_clean():
    return deduplicate_by_window(normalize_group_retirement_members(), ["member_id"], ["_ingested_at"])


def build_reinsurance_treaty_clean():
    return deduplicate_by_window(normalize_reinsurance_treaties(), ["treaty_id"], ["_ingested_at"])


def build_schema_drift_log():
    drift_rows = []
    for source_name_value in BRONZE_TABLES:
        current_columns, unexpected_columns, missing_columns = schema_signature(source_name_value)
        if unexpected_columns:
            drift_rows.append((source_name_value, str(unexpected_columns), "UNEXPECTED_COLUMNS"))
        if missing_columns:
            drift_rows.append((source_name_value, str(missing_columns), "MISSING_EXPECTED_COLUMNS"))
        if source_name_value == "adobe_analytics.digital_events" and "[" in current_columns:
            drift_rows.append((source_name_value, "Adobe bronze contains raw JSON lines in a single '[' column; reparsing raw JSON file path in silver.", "RAW_JSON_LINE_SPLIT"))

    drift_schema = T.StructType([
        T.StructField("source_name", T.StringType(), True),
        T.StructField("details", T.StringType(), True),
        T.StructField("drift_type", T.StringType(), True),
    ])
    drift_df = spark.createDataFrame(drift_rows, drift_schema) if drift_rows else spark.createDataFrame([], drift_schema)
    return drift_df.withColumn("detected_at", F.current_timestamp()).withColumn("run_id", F.lit(run_id))


def build_dedup_audit_log():
    if DEDUP_AUDIT_CACHE is None:
        build_target_dataframe("policy.individual_life_clean")
    if DEDUP_AUDIT_CACHE is None:
        schema = T.StructType([
            T.StructField("policy_number", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("_source_system", T.StringType(), True),
            T.StructField("_ingested_at", T.TimestampType(), True),
            T.StructField("_batch_id", T.StringType(), True),
            T.StructField("_dedup_rank", T.IntegerType(), True),
        ])
        empty_df = spark.createDataFrame([], schema)
        return empty_df.withColumn("run_id", F.lit(run_id))
    return DEDUP_AUDIT_CACHE.withColumn("run_id", F.lit(run_id))


def build_allocation_errors():
    if ALLOCATION_ERROR_CACHE is None:
        build_target_dataframe("investments.fund_allocation_detail")
    if ALLOCATION_ERROR_CACHE is None:
        schema = T.StructType([
            T.StructField("contract_number", T.StringType(), True),
            T.StructField("total_allocation_pct", T.DoubleType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("detected_at", T.TimestampType(), True),
        ])
        return spark.createDataFrame([], schema)
    return ALLOCATION_ERROR_CACHE


TARGET_BUILDERS = {
    "customer.master": build_customer_master,
    "policy.individual_life_clean": build_policy_individual_life_clean,
    "policy.disability_ci_clean": build_policy_disability_ci_clean,
    "reference.product_code_mapping": build_reference_product_code_mapping,
    "reference.status_code_mapping": build_reference_status_code_mapping,
    "reference.rider_codes": build_reference_rider_codes,
    "policy.individual_life_enriched": build_policy_individual_life_enriched,
    "policy.policy_rider_detail": build_policy_rider_detail,
    "digital.portal_clean": build_digital_portal_clean,
    "interactions.callcentre_clean": normalize_callcentre_interactions,
    "group_benefits.plan_clean": build_group_benefits_plan_clean,
    "group_benefits.certificate_clean": build_group_benefits_certificate_clean,
    "group_benefits.certificate_coverage_detail": build_group_benefits_certificate_coverage_detail,
    "freedom55.advisor_feed_clean": build_freedom55_advisor_feed_clean,
    "investments.climl_clean": build_investments_climl_clean,
    "investments.fund_allocation_detail": build_investments_fund_allocation_detail,
    "group_retirement.member_clean": build_group_retirement_member_clean,
    "reinsurance.treaty_clean": build_reinsurance_treaty_clean,
    "monitoring.schema_drift_log": build_schema_drift_log,
    "monitoring.dedup_audit_log": build_dedup_audit_log,
    "monitoring.allocation_errors": build_allocation_errors,
}


def build_target_dataframe(target_name_value: str):
    if target_name_value in DATAFRAME_CACHE:
        return DATAFRAME_CACHE[target_name_value]

    builder = TARGET_BUILDERS.get(target_name_value)
    if builder is None:
        raise ValueError(f"Unsupported target: {target_name_value}")

    df = builder()
    DATAFRAME_CACHE[target_name_value] = df
    return df

# ------------------------------------------------------------------------------
# 6. Execution
# ------------------------------------------------------------------------------
selected_targets = resolve_selected_targets()
ensure_silver_schema()
result_rows = []

if execution_mode == "PLAN":
    for target_name_value in selected_targets:
        target_config = TARGET_CONFIG[target_name_value]
        source_details = []
        adobe_json_flag = False
        for source_value in target_config["sources"]:
            if source_value in BRONZE_TABLES:
                source_fqn = bronze_table_fqn(source_value)
                exists_flag = table_exists(source_fqn)
                source_details.append(f"{source_value} -> {source_fqn} (exists={exists_flag})")
                if source_value == "adobe_analytics.digital_events":
                    adobe_json_flag = True
            else:
                source_details.append(f"{source_value} -> derived silver dependency")
        result_rows.append({
            "target_name": target_name_value,
            "kind": target_config["kind"],
            "write_mode": target_config["write_mode"],
            "sources": " | ".join(source_details),
            "json_flattening": "ADOBE_ONLY" if adobe_json_flag else "NO_JSON_FLATTENING",
            "target_table": silver_table_fqn(target_name_value),
            "target_path": silver_storage_path(target_name_value),
            "status": "READY",
        })
    display(spark.createDataFrame(result_rows))
else:
    for target_name_value in selected_targets:
        target_config = TARGET_CONFIG[target_name_value]
        try:
            target_df = build_target_dataframe(target_name_value)
            target_count = target_df.count()
            try:
                preview_rows = target_df.limit(5).collect()
                preview_df = spark.createDataFrame(preview_rows, target_df.schema) if preview_rows else spark.createDataFrame([], target_df.schema)
                display(preview_df)
            except Exception as preview_exc:
                print(f"PREVIEW_SKIPPED|{target_name_value}|{type(preview_exc).__name__}: {str(preview_exc)}")

            if execution_mode == "RUN":
                if target_config["write_mode"] == "scd2":
                    target_df = apply_scd2(target_df, target_name_value, target_config["keys"][0])
                else:
                    write_delta(target_df, target_name_value, target_config["write_mode"])
                maybe_optimize(target_name_value, target_config["keys"])

            result_rows.append({
                "target_name": target_name_value,
                "kind": target_config["kind"],
                "write_mode": target_config["write_mode"],
                "row_count": target_count,
                "target_table": silver_table_fqn(target_name_value),
                "target_path": silver_storage_path(target_name_value),
                "status": "SUCCESS",
                "message": f"Processed {target_count:,} rows in {execution_mode} mode",
            })
        except Exception as exc:
            error_message = f"{type(exc).__name__}: {str(exc)}"
            print(f"TARGET_FAILED|{target_name_value}|{error_message}")
            result_rows.append({
                "target_name": target_name_value,
                "kind": target_config["kind"],
                "write_mode": target_config["write_mode"],
                "row_count": None,
                "target_table": silver_table_fqn(target_name_value),
                "target_path": silver_storage_path(target_name_value),
                "status": "FAILED",
                "message": error_message,
            })

    result_schema = T.StructType([
        T.StructField("target_name", T.StringType(), True),
        T.StructField("kind", T.StringType(), True),
        T.StructField("write_mode", T.StringType(), True),
        T.StructField("row_count", T.LongType(), True),
        T.StructField("target_table", T.StringType(), True),
        T.StructField("target_path", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("message", T.StringType(), True),
    ])
    result_df = spark.createDataFrame(result_rows, result_schema)
    display(result_df)

    failed_rows = [row for row in result_rows if row.get("status") == "FAILED"]
    if failed_rows:
        print("FAILED_TARGET_SUMMARY_START")
        for failed_row in failed_rows:
            print(
                f"FAILED_TARGET|{failed_row['target_name']}|{failed_row['write_mode']}|{failed_row['message']}"
            )
        print("FAILED_TARGET_SUMMARY_END")
        failed_target_names = ", ".join(row["target_name"] for row in failed_rows)
        print(f"FAILED_TARGET_NAMES={failed_target_names}")


# COMMAND ----------

# DBTITLE 1,Debug policy clean failure
DATAFRAME_CACHE = {}
DEDUP_AUDIT_CACHE = None
ALLOCATION_ERROR_CACHE = None
IDENTITY_AUTO_MERGE_CACHE = None
IDENTITY_REVIEW_QUEUE_CACHE = None

try:
    debug_policy_df = build_target_dataframe("policy.individual_life_clean")
    print("debug_policy_columns=" + ", ".join(debug_policy_df.columns))
    print(f"debug_policy_count={debug_policy_df.count()}")
    display(debug_policy_df.limit(5))
except Exception as exc:
    import traceback
    print(type(exc).__name__)
    print(str(exc))
    traceback.print_exc(limit=20)


# COMMAND ----------

# DBTITLE 1,Inspect failed execution summary
failed_rows = [row for row in result_rows if row.get("status") == "FAILED"]
failed_rows_schema = T.StructType([
    T.StructField("target_name", T.StringType(), True),
    T.StructField("kind", T.StringType(), True),
    T.StructField("write_mode", T.StringType(), True),
    T.StructField("row_count", T.LongType(), True),
    T.StructField("target_table", T.StringType(), True),
    T.StructField("target_path", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("message", T.StringType(), True),
])
failed_rows_df = spark.createDataFrame(failed_rows, failed_rows_schema)
display(failed_rows_df)
print(f"failed_target_count={len(failed_rows)}")

DATAFRAME_CACHE = {}
DEDUP_AUDIT_CACHE = None
ALLOCATION_ERROR_CACHE = None
enriched_df = build_target_dataframe("policy.individual_life_enriched")
duplicate_columns = sorted({column_name for column_name in enriched_df.columns if enriched_df.columns.count(column_name) > 1})
print("enriched_duplicate_columns=" + ", ".join(duplicate_columns))
print("enriched_columns=" + ", ".join(enriched_df.columns))

# COMMAND ----------

# DBTITLE 1,Silver expectation spot checks
# ------------------------------------------------------------------------------
# 7. Silver expectation spot checks
# ------------------------------------------------------------------------------
spot_check_run_date = run_date or "2026-06-08"
silver_tables_expected = [
    "customer_master",
    "digital_portal_clean",
    "freedom55_advisor_feed_clean",
    "group_benefits_plan_clean",
    "group_benefits_certificate_clean",
    "group_benefits_certificate_coverage_detail",
    "group_retirement_member_clean",
    "interactions_callcentre_clean",
    "investments_climl_clean",
    "investments_fund_allocation_detail",
    "monitoring_allocation_errors",
    "monitoring_dedup_audit_log",
    "monitoring_schema_drift_log",
    "policy_disability_ci_clean",
    "policy_individual_life_clean",
    "policy_individual_life_enriched",
    "policy_policy_rider_detail",
    "reference_product_code_mapping",
    "reference_rider_codes",
    "reference_status_code_mapping",
    "reinsurance_treaty_clean",
]

summary_rows = []
for table_name in silver_tables_expected:
    fqn = f"{catalog_name}.{silver_schema}.{table_name}"
    exists_flag = table_exists(fqn)
    row_count = spark.table(fqn).count() if exists_flag else None
    summary_rows.append((table_name, exists_flag, row_count))

summary_df = spark.createDataFrame(summary_rows, ["table_name", "exists", "row_count"])
display(summary_df.orderBy("table_name"))

customer_df = spark.table(silver_table_fqn("customer.master"))
policy_clean_df = spark.table(silver_table_fqn("policy.individual_life_clean"))
policy_enriched_df = spark.table(silver_table_fqn("policy.individual_life_enriched"))
digital_df = spark.table(silver_table_fqn("digital.portal_clean"))
call_df = spark.table(silver_table_fqn("interactions.callcentre_clean"))
f55_df = spark.table(silver_table_fqn("freedom55.advisor_feed_clean"))
retirement_df = spark.table(silver_table_fqn("group_retirement.member_clean"))
rider_df = spark.table(silver_table_fqn("policy.policy_rider_detail"))
alloc_df = spark.table(silver_table_fqn("investments.fund_allocation_detail"))
status_ref_df = spark.table(silver_table_fqn("reference.status_code_mapping"))
product_ref_df = spark.table(silver_table_fqn("reference.product_code_mapping"))
drift_df = spark.table(silver_table_fqn("monitoring.schema_drift_log"))

checks = [
    ("all_21_tables_present", summary_df.filter(~F.col("exists")).count() == 0),
    ("customer_masked_email_present", customer_df.filter(F.col("email_clean").rlike(r"^..\*\*\*@")).count() > 0),
    ("customer_masked_phone_present", customer_df.filter(F.col("phone_clean").rlike(r"^\*\*\*-\*\*\*-\d{4}$")).count() > 0),
    ("customer_postal_normalized", customer_df.filter(F.col("postal_code_clean").contains(" ")).count() == 0),
    ("customer_province_two_letter", customer_df.filter((F.col("province_clean").isNotNull()) & (F.length(F.col("province_clean")) != 2)).count() == 0),
    ("policy_scd2_columns_present", set(["effective_date", "expiry_date", "is_current"]).issubset(set(policy_clean_df.columns))),
    ("policy_current_rows_present", policy_clean_df.filter(F.col("is_current") == True).count() > 0),
    ("policy_effective_date_matches_run_date", policy_clean_df.filter(F.col("effective_date") == F.to_date(F.lit(spot_check_run_date))).count() > 0),
    ("policy_canonical_product_present", policy_enriched_df.filter(F.col("product_type_code_canonical").isNotNull()).count() > 0),
    ("policy_canonical_status_present", policy_enriched_df.filter(F.col("policy_status_canonical").isNotNull()).count() > 0),
    ("digital_contains_both_sources", digital_df.select("_source_system").distinct().count() >= 2),
    ("callcentre_duration_present", call_df.filter(F.col("call_duration_minutes").isNotNull()).count() > 0),
    ("freedom55_assignment_status_present", f55_df.filter(F.col("advisor_assignment_status").isNotNull()).count() > 0),
    ("group_retirement_status_present", retirement_df.filter(F.col("member_status").isNotNull()).count() > 0),
    ("rider_detail_generated", rider_df.count() > 0),
    ("allocation_detail_generated", alloc_df.count() > 0),
    ("status_reference_generated", status_ref_df.count() > 0),
    ("product_reference_generated", product_ref_df.count() > 0),
    ("monitoring_schema_drift_generated", drift_df.count() >= 0),
    ("identity_resolution_columns_present", set(["postal_code_clean", "date_of_birth"]).issubset(set(policy_clean_df.columns))),
    ("manual_review_queue_present", table_exists(f"{catalog_name}.compliance.identity_resolution_manual_review")),
]

checks_df = spark.createDataFrame(checks, ["check_name", "passed"])
display(checks_df.orderBy("check_name"))

samples = [
    ("customer_master", str(customer_df.select("customer_id", "email_clean", "phone_clean", "province_clean", "postal_code_clean").limit(1).collect()[0].asDict())),
    ("policy_individual_life_clean", str(policy_clean_df.select("policy_number", "legacy_policy_number", "product_type_code", "policy_status_code", "beneficiary_id", "rider_codes", "underwriting_class_code", "effective_date", "expiry_date", "is_current").limit(1).collect()[0].asDict())),
    ("policy_individual_life_enriched", str(policy_enriched_df.select("policy_number", "product_type_code", "product_type_code_canonical", "policy_status_code", "policy_status_canonical", "annualised_premium", "churn_risk_signal").limit(1).collect()[0].asDict())),
    ("digital_portal_clean", str(digital_df.select("event_id", "customer_id", "event_type", "_source_system").limit(1).collect()[0].asDict())),
    ("interactions_callcentre_clean", str(call_df.select("interaction_id", "customer_id", "agent_id", "interaction_status", "call_duration_minutes").limit(1).collect()[0].asDict())),
    ("freedom55_advisor_feed_clean", str(f55_df.select("assignment_id", "advisor_id", "customer_id", "advisor_assignment_status", "region", "channel").limit(1).collect()[0].asDict())),
    ("group_retirement_member_clean", str(retirement_df.select("member_id", "plan_id", "employer_id", "contribution_amount", "vesting_status", "member_status").limit(1).collect()[0].asDict())),
]
samples_df = spark.createDataFrame(samples, ["table_name", "sample_value"])
display(samples_df)

failed_checks = [row[0] for row in checks_df.filter(F.col("passed") == False).collect()]
print("DIGITAL_SOURCE_SYSTEMS=" + ", ".join(sorted([row[0] for row in digital_df.select("_source_system").distinct().collect()])))
print("FAILED_EXPECTATION_CHECKS=" + (", ".join(failed_checks) if failed_checks else "NONE"))
if failed_checks:
    print("SILVER_EXPECTATION_STATUS=PARTIAL")
else:
    print("SILVER_EXPECTATION_STATUS=PASS")


# COMMAND ----------

# DBTITLE 1,Debug downstream silver targets
DATAFRAME_CACHE = {}
DEDUP_AUDIT_CACHE = None
ALLOCATION_ERROR_CACHE = None
IDENTITY_AUTO_MERGE_CACHE = None
IDENTITY_REVIEW_QUEUE_CACHE = None

for debug_target_name in ["policy.individual_life_clean", "policy.individual_life_enriched", "policy.policy_rider_detail"]:
    try:
        debug_target_df = build_target_dataframe(debug_target_name)
        print(f"DEBUG_TARGET={debug_target_name}")
        print("debug_target_columns=" + ", ".join(debug_target_df.columns))
        print(f"debug_target_count={debug_target_df.count()}")
        try:
            debug_preview_rows = debug_target_df.limit(5).collect()
            debug_preview_df = spark.createDataFrame(debug_preview_rows, debug_target_df.schema) if debug_preview_rows else spark.createDataFrame([], debug_target_df.schema)
            display(debug_preview_df)
        except Exception as preview_exc:
            import traceback
            print(f"DEBUG_PREVIEW_FAILED={debug_target_name}")
            print(type(preview_exc).__name__)
            print(str(preview_exc))
            traceback.print_exc(limit=20)
    except Exception as exc:
        import traceback
        print(f"DEBUG_TARGET={debug_target_name}")
        print(type(exc).__name__)
        print(str(exc))
        traceback.print_exc(limit=20)
