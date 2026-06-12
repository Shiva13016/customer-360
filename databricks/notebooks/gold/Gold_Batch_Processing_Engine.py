# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC This notebook builds the Canada Life Customer 360 gold layer from silver sources.
# MAGIC
# MAGIC Gold outputs provide purpose-built business views (5 tables managed here; 6th table `pipeda_audit` maintained separately in [Compliance Audit Notebook](#notebook-2704371916535664)):
# MAGIC
# MAGIC * **customer_360**: Wide denormalised fact with 1 row/customer, current state, multi-source LEFT JOINs, cross-sell propensity signals
# MAGIC * **regulatory_view**: Aggregate fact for OSFI B-10 reporting with gross/net face amounts and premiums by province and product
# MAGIC * **ml_features**: Feature store with full SCD2 history for `lapse_count_lifetime` and `reinstatement_count`, no PII, `c360_customer_id` only
# MAGIC * **book_of_business**: Fact with Unity Catalog RLS row filter on `advisor_id` â includes disability_gap_flag and ci_cross_sell_flag
# MAGIC * **kpi_summary**: Daily KPIs aggregate including 30d churn rate using SCD2 `effective_date`
# MAGIC
# MAGIC All gold tables write to:
# MAGIC * Unity Catalog: `dbw_c360_canadalife.gold.<table_name>` (single gold schema per project preferences)
# MAGIC * ADLS Gen2: `abfss://gold@adlsc360canadalife.dfs.core.windows.net/gold/<table_name>/` (custom ADLS paths)
# MAGIC
# MAGIC Execution modes:
# MAGIC * `PLAN` inspects configured targets without reading full outputs
# MAGIC * `TEST` builds each target and previews rows without writing
# MAGIC * `RUN` builds and writes gold outputs to Unity Catalog and ADLS
# MAGIC
# MAGIC Active runtime widgets:
# MAGIC * `target_table_name`: choose one configured gold target to run, or use `ALL` to run the full gold pipeline (5 tables)
# MAGIC * `run_date`: optional date filter for incremental gold refresh (defaults to current date)
# MAGIC * `execution_mode`: controls whether the notebook plans, tests, or writes outputs
# MAGIC * `optimize_output`: when `true`, runs `OPTIMIZE` on written gold Delta tables in `RUN` mode
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parameters and imports
# ==============================================================================
# Notebook: Gold_Batch_Processing_Engine
# Purpose   : Silver -> Gold business-ready outputs for Canada Life Customer 360
#             with 5 gold tables (6th table pipeda_audit maintained separately)
# Notes     :
#             * All gold tables write to custom ADLS paths under gold container
#             * Single gold schema consolidates all consumer-facing outputs
#             * Default execution_mode is PLAN so the notebook is safe to run first
# ==============================================================================
import re
import uuid
from functools import reduce
from datetime import datetime, timedelta

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
]:
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

DEFAULT_CATALOG_NAME = "dbw_c360_canadalife"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"
DEFAULT_GOLD_BASE_PATH = "abfss://gold@adlsc360canadalife.dfs.core.windows.net/gold"

widget_defaults = {
    "target_table_name": "ALL",
    "run_date": "",
    "execution_mode": "PLAN",   # PLAN | TEST | RUN
    "optimize_output": "false",
}

for widget_name, default_value in widget_defaults.items():
    dbutils.widgets.text(widget_name, default_value)

catalog_name = DEFAULT_CATALOG_NAME
silver_schema = DEFAULT_SILVER_SCHEMA
gold_schema = DEFAULT_GOLD_SCHEMA
target_table_name = dbutils.widgets.get("target_table_name").strip() or "ALL"
run_date = dbutils.widgets.get("run_date").strip()
gold_base_path = DEFAULT_GOLD_BASE_PATH
execution_mode = (dbutils.widgets.get("execution_mode").strip() or "PLAN").upper()
optimize_output = dbutils.widgets.get("optimize_output").strip().lower() == "true"
run_id = str(uuid.uuid4())

if execution_mode not in {"PLAN", "TEST", "RUN"}:
    raise ValueError("execution_mode must be one of PLAN, TEST, or RUN")


# COMMAND ----------

# DBTITLE 1,Configuration summary
# MAGIC %md
# MAGIC This section centralizes notebook metadata:
# MAGIC * silver source table mappings
# MAGIC * target definitions and write modes
# MAGIC * runtime caches for reusable datasets
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuration and target metadata
# ------------------------------------------------------------------------------
# 2. Constants and Configuration
# ------------------------------------------------------------------------------
SILVER_TABLES = {
    "customer.master": "customer_master",
    "policy.individual_life_clean": "policy_individual_life_clean",
    "policy.individual_life_enriched": "policy_individual_life_enriched",
    "policy.disability_ci_clean": "policy_disability_ci_clean",
    "policy.policy_rider_detail": "policy_policy_rider_detail",
    "digital.portal_clean": "digital_portal_clean",
    "interactions.callcentre_clean": "interactions_callcentre_clean",
    "group_benefits.plan_clean": "group_benefits_plan_clean",
    "group_benefits.certificate_clean": "group_benefits_certificate_clean",
    "group_benefits.certificate_coverage_detail": "group_benefits_certificate_coverage_detail",
    "freedom55.advisor_feed_clean": "freedom55_advisor_feed_clean",
    "investments.climl_clean": "investments_climl_clean",
    "investments.fund_allocation_detail": "investments_fund_allocation_detail",
    "group_retirement.member_clean": "group_retirement_member_clean",
    "reinsurance.treaty_clean": "reinsurance_treaty_clean",
    "reference.product_code_mapping": "reference_product_code_mapping",
    "reference.status_code_mapping": "reference_status_code_mapping",
    "reference.rider_codes": "reference_rider_codes",
}

# ------------------------------------------------------------------------------
# Gold target configuration for the 5 business-ready outputs managed by this
# notebook. The 6th output (pipeda_audit) is maintained separately in the
# Compliance Audit Notebook (ID: 2704371916535664) and writes to the single
# gold schema.
# ------------------------------------------------------------------------------
TARGET_CONFIG = {
    "customer_360": {
        "kind": "business",
        "sources": [
            "customer.master",
            "policy.individual_life_enriched",
            "policy.disability_ci_clean",
            "group_benefits.certificate_coverage_detail",
            "group_retirement.member_clean",
            "investments.climl_clean",
            "interactions.callcentre_clean",
        ],
        "keys": ["customer_id"],
        "write_mode": "overwrite",
        "description": "Wide denormalised fact, 1 row/customer, current state with cross-sell propensity signals",
    },
    "regulatory_view": {
        "kind": "business",
        "sources": [
            "policy.individual_life_enriched",
            "reinsurance.treaty_clean",
        ],
        "keys": ["product_type_code_canonical", "province_clean", "policy_status_canonical"],
        "write_mode": "overwrite",
        "description": "Aggregate fact for OSFI B-10 reporting, gross/net face amounts and premiums by province",
    },
    "ml_features": {
        "kind": "business",
        "sources": [
            "policy.individual_life_clean",
            "digital.portal_clean",
            "group_retirement.member_clean",
        ],
        "keys": ["customer_id"],
        "write_mode": "overwrite",
        "description": "Feature store with full SCD2 history for lapse_count_lifetime and reinstatement_count, no PII",
    },
    "book_of_business": {
        "kind": "business",
        "sources": [
            "customer.master",
            "freedom55.advisor_feed_clean",
            "policy.individual_life_enriched",
        ],
        "keys": ["advisor_id", "customer_id"],
        "write_mode": "overwrite",
        "description": "Fact with Unity Catalog RLS row filter on advisor_id",
    },
    "kpi_summary": {
        "kind": "business",
        "sources": [
            "policy.individual_life_clean",
        ],
        "keys": ["kpi_date"],
        "write_mode": "overwrite",
        "description": "Daily KPIs aggregate including 30d churn rate using SCD2 effective_date",
    },
}

TARGET_ORDER = [
    "customer_360",
    "regulatory_view",
    "ml_features",
    "book_of_business",
    "kpi_summary",
]

DATAFRAME_CACHE = {}


# COMMAND ----------

# DBTITLE 1,Shared helpers
# MAGIC %md
# MAGIC These helper functions are reused across many targets.
# MAGIC
# MAGIC Highlights:
# MAGIC * target and path name resolution
# MAGIC * silver source reading with caching
# MAGIC * Delta write helpers for custom ADLS paths
# MAGIC * shared aggregation and window functions
# MAGIC

# COMMAND ----------

# DBTITLE 1,Utility functions
# ------------------------------------------------------------------------------
# 3. Utility Functions
# ------------------------------------------------------------------------------
def logical_to_physical(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", name.strip()).strip("_").lower()


def silver_table_fqn(source_name_value: str) -> str:
    return f"{catalog_name}.{silver_schema}.{SILVER_TABLES[source_name_value]}"


def gold_table_fqn(target_name_value: str) -> str:
    return f"{catalog_name}.{gold_schema}.{logical_to_physical(target_name_value)}"


def gold_storage_path(target_name_value: str) -> str:
    return f"{gold_base_path}/{logical_to_physical(target_name_value)}"


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def ensure_gold_schema() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")


def normalise_filter_value(name: str) -> str:
    return logical_to_physical(name)


def resolve_selected_targets() -> list:
    if target_table_name.upper() != "ALL":
        selected = [target_name for target_name in TARGET_ORDER if normalise_filter_value(target_name) == normalise_filter_value(target_table_name)]
        if not selected:
            raise ValueError(f"Target '{target_table_name}' is not configured.")
        return selected
    return TARGET_ORDER


def read_silver_source(source_name_value: str):
    if source_name_value in DATAFRAME_CACHE:
        return DATAFRAME_CACHE[source_name_value]
    df = spark.table(silver_table_fqn(source_name_value))
    DATAFRAME_CACHE[source_name_value] = df
    return df


def ensure_external_table(target_name_value: str):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {gold_table_fqn(target_name_value)} USING DELTA LOCATION '{gold_storage_path(target_name_value)}'"
    )


def write_delta(df, target_name_value: str, mode: str):
    path = gold_storage_path(target_name_value)
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
    table_name = gold_table_fqn(target_name_value)
    if valid_columns:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({', '.join(valid_columns)})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")


def build_target_dataframe(target_name_value: str):
    if target_name_value == "customer_360":
        return build_customer_360()
    elif target_name_value == "regulatory_view":
        return build_regulatory_view()
    elif target_name_value == "ml_features":
        return build_ml_features()
    elif target_name_value == "book_of_business":
        return build_book_of_business()
    elif target_name_value == "kpi_summary":
        return build_kpi_summary()
    else:
        raise ValueError(f"Unknown target: {target_name_value}")


# COMMAND ----------

# DBTITLE 1,Target builders and orchestration
# MAGIC %md
# MAGIC This section assembles silver sources into the final gold outputs.
# MAGIC
# MAGIC It includes:
# MAGIC * business target builders for each gold table
# MAGIC * the main execution loop for PLAN, TEST, and RUN
# MAGIC

# COMMAND ----------

# DBTITLE 1,Target builders and execution
# ------------------------------------------------------------------------------
# 4. Target Builders for Canada Life Customer 360 Gold Tables
#
# Five business-ready outputs (6th output, pipeda_audit, maintained separately
# in Compliance Audit Notebook ID: 2704371916535664):
#
# G1: customer_360 - Wide denormalised fact, 1 row/customer, current state
# G2: regulatory_view - Aggregate fact for OSFI B-10 compliance  
# G3: ml_features - Feature store with full SCD2 history, no PII
# G4: book_of_business - Fact with Unity Catalog RLS row filter on advisor_id
# G5: kpi_summary - Daily KPIs aggregate
# ------------------------------------------------------------------------------
def build_customer_360():
    """
    G1: Wide denormalised fact with 1 row/customer, current state, multi-source LEFT JOINs,
    cross-sell propensity signals.
    """
    customer_df = read_silver_source("customer.master")
    policy_enriched_df = read_silver_source("policy.individual_life_enriched")
    disability_df = read_silver_source("policy.disability_ci_clean")
    group_benefits_df = read_silver_source("group_benefits.certificate_coverage_detail")
    group_retirement_df = read_silver_source("group_retirement.member_clean")
    investments_df = read_silver_source("investments.climl_clean")
    callcentre_df = read_silver_source("interactions.callcentre_clean")
    
    # Base: customer master
    base_df = customer_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email_clean"),
        F.col("phone_clean"),
        F.col("province_clean"),
        F.col("postal_code_clean"),
        F.col("channel"),
        F.col("advisor_id"),
    )
    
    # Aggregate policy metrics using the live silver contract.
    # `policy_status_canonical` is stored as compact codes (A, C, S, L, P), not words.
    # Exclude terminal life statuses so current coverage metrics do not count lapsed/cancelled policies.
    policy_current_df = policy_enriched_df.withColumn(
        "policy_status_canonical_normalized",
        F.upper(F.trim(F.coalesce(F.col("policy_status_canonical"), F.lit(""))))
    ).filter(
        ~F.col("policy_status_canonical_normalized").isin("L", "C")
    )

    policy_agg_df = policy_current_df.groupBy("customer_id").agg(
        F.count("*").alias("life_policy_count"),
        F.sum("face_amount").alias("total_life_face_amount"),
        F.sum("annualised_premium").alias("total_life_premium"),
        F.max(F.col("term_expiring_90d_flag").cast("int")).alias("has_term_expiring_90d"),
        F.min("issue_date").alias("first_policy_date"),
    )
    
    # Disability/CI coverage flags using current non-terminal records from the live silver contract.
    disability_current_df = disability_df.withColumn(
        "policy_status_code_normalized",
        F.upper(F.trim(F.coalesce(F.col("policy_status_code"), F.lit(""))))
    ).filter(
        ~F.col("policy_status_code_normalized").isin("LAPSED", "CANCELLED")
    )

    disability_agg_df = disability_current_df.groupBy("customer_id").agg(
        F.when(
            F.count("*") > 0, F.lit(True)
        ).otherwise(F.lit(False)).alias("has_disability_coverage"),
        F.when(
            F.sum(F.when(F.col("product_type_code").rlike("(?i)CI|CRITICAL"), 1).otherwise(0)) > 0,
            F.lit(True)
        ).otherwise(F.lit(False)).alias("has_ci_coverage"),
    )
    
    # Group benefits: active members only
    group_benefits_agg_df = group_benefits_df.filter(
        F.col("termination_date").isNull()
    ).groupBy(
        F.col("member_id").alias("customer_id")
    ).agg(
        F.countDistinct("plan_id").alias("group_benefits_plan_count"),
    )
    
    # Group retirement: normalize live status values before current-state filtering.
    group_retirement_agg_df = group_retirement_df.withColumn(
        "member_status_normalized",
        F.upper(F.trim(F.coalesce(F.col("member_status"), F.lit(""))))
    ).filter(
        F.col("member_status_normalized") == "ACTIVE"
    ).groupBy(
        F.col("member_id").alias("customer_id")
    ).agg(
        F.sum("contribution_amount").alias("total_retirement_contribution"),
    )
    
    # Investments
    investments_agg_df = investments_df.groupBy("customer_id").agg(
        F.sum("market_value").alias("total_investment_market_value"),
    )

    # Join-key coverage note from live silver validation:
    # policy_individual_life_enriched.customer_id overlaps with customer_master.customer_id,
    # but the current group benefits, group retirement, investments, disability, and callcentre
    # tables are on different identifier domains in the live data. Keep the LEFT JOIN structure
    # intact for now so G1 remains runnable while Phase 1 continues toward contract alignment.
    
    # Call center: last interaction
    callcentre_agg_df = callcentre_df.groupBy("customer_id").agg(
        F.max("call_start_ts").alias("last_callcentre_contact"),
        F.count("*").alias("callcentre_interaction_count"),
    )
    
    # LEFT JOINs
    c360_df = base_df.join(
        policy_agg_df, ["customer_id"], "left"
    ).join(
        disability_agg_df, ["customer_id"], "left"
    ).join(
        group_benefits_agg_df, ["customer_id"], "left"
    ).join(
        group_retirement_agg_df, ["customer_id"], "left"
    ).join(
        investments_agg_df, ["customer_id"], "left"
    ).join(
        callcentre_agg_df, ["customer_id"], "left"
    )
    
    # Cross-sell propensity signals
    c360_df = c360_df.withColumn(
        "cross_sell_propensity_ci",
        F.when(
            (F.col("has_ci_coverage").isNull() | (F.col("has_ci_coverage") == False))
            & (F.months_between(F.current_date(), F.col("first_policy_date")) >= 6),
            F.lit(True)
        ).otherwise(F.lit(False))
    ).withColumn(
        "cross_sell_term_conversion_flag",
        F.coalesce(F.col("has_term_expiring_90d"), F.lit(0)).cast("boolean")
    )
    
    # Add gold metadata
    result_df = c360_df.withColumn(
        "gold_ingested_at", F.current_timestamp()
    ).withColumn(
        "gold_run_id", F.lit(run_id)
    )
    
    return result_df


def build_regulatory_view():
    """
    G2: Aggregate fact for OSFI B-10 compliance, gross/net face amounts and premiums
    by province and product.
    """
    policy_enriched_df = read_silver_source("policy.individual_life_enriched")
    reinsurance_df = read_silver_source("reinsurance.treaty_clean")
    
    # Policy aggregates using the live compact canonical status values.
    policy_current_df = policy_enriched_df.withColumn(
        "policy_status_canonical_normalized",
        F.upper(F.trim(F.coalesce(F.col("policy_status_canonical"), F.lit(""))))
    ).filter(
        ~F.col("policy_status_canonical_normalized").isin("L", "C")
    )

    policy_agg_df = policy_current_df.groupBy(
        F.col("product_type_code_canonical"),
        F.col("province_clean"),
        F.col("policy_status_canonical"),
    ).agg(
        F.count("*").alias("policy_count"),
        F.sum("face_amount").alias("gross_face_amount"),
        F.sum("annualised_premium").alias("gross_annualised_premium"),
        F.sum(
            F.when(F.col("issue_date") >= F.lit("2023-01-01"), F.col("annualised_premium")).otherwise(0)
        ).alias("ifrs17_premium"),
    )
    
    # Reinsurance ceded amounts (join on policy_number, aggregate to same grain)
    reinsurance_agg_df = reinsurance_df.groupBy(
        F.col("policy_number"),
        F.col("product_type_code"),
        F.col("policy_status_code"),
    ).agg(
        F.sum("ceded_amount").alias("ceded_face_amount"),
        F.sum("premium_ceded").alias("ceded_premium"),
    )
    
    # Join policy and reinsurance at policy level, then re-aggregate.
    # Keep this on the same current-state life slice used in policy_agg_df.
    policy_reinsurance_df = policy_current_df.join(
        reinsurance_agg_df,
        (policy_current_df["policy_number"] == reinsurance_agg_df["policy_number"])
        & (policy_current_df["product_type_code"] == reinsurance_agg_df["product_type_code"]),
        "left"
    ).groupBy(
        F.col("product_type_code_canonical"),
        F.col("province_clean"),
        F.col("policy_status_canonical"),
    ).agg(
        F.sum("ceded_face_amount").alias("total_ceded_face_amount"),
        F.sum("ceded_premium").alias("total_ceded_premium"),
    )
    
    # Merge policy aggregates with ceded amounts
    regulatory_df = policy_agg_df.join(
        policy_reinsurance_df,
        ["product_type_code_canonical", "province_clean", "policy_status_canonical"],
        "left"
    ).withColumn(
        "net_face_amount",
        F.col("gross_face_amount") - F.coalesce(F.col("total_ceded_face_amount"), F.lit(0))
    ).withColumn(
        "net_annualised_premium",
        F.col("gross_annualised_premium") - F.coalesce(F.col("total_ceded_premium"), F.lit(0))
    )
    
    # Add gold metadata
    result_df = regulatory_df.withColumn(
        "gold_ingested_at", F.current_timestamp()
    ).withColumn(
        "gold_run_id", F.lit(run_id)
    )
    
    return result_df


def build_ml_features():
    """
    G3: Feature store with full SCD2 history for lapse_count_lifetime, reinstatement_count,
    no PII, c360_customer_id only.
    """
    policy_clean_df = read_silver_source("policy.individual_life_clean")
    digital_df = read_silver_source("digital.portal_clean")
    retirement_df = read_silver_source("group_retirement.member_clean")
    
    # Policy features: use full SCD2 history (NO is_current filter), normalize status codes
    # before lapse/reinstatement pattern detection.
    policy_with_normalized_status = policy_clean_df.withColumn(
        "policy_status_normalized",
        F.upper(F.trim(F.coalesce(F.col("policy_status_code"), F.lit(""))))
    )

    policy_features_df = policy_with_normalized_status.groupBy("customer_id").agg(
        F.count("*").alias("total_policy_history_count"),
        F.sum(
            F.when(F.col("policy_status_normalized").isin("L", "LAPSED"), 1).otherwise(0)
        ).alias("lapse_count_lifetime"),
        F.sum(
            F.when(F.col("policy_status_normalized").isin("R", "REINSTATED"), 1).otherwise(0)
        ).alias("reinstatement_count"),
        F.min("issue_date").alias("first_policy_issue_date"),
    )
    
    # Digital engagement score (computed from event_type counts, no PII)
    digital_features_df = digital_df.groupBy("customer_id").agg(
        F.sum(
            F.when(F.col("event_type") == "login", 1).otherwise(0)
        ).alias("logins_count"),
        F.sum(
            F.when(F.col("event_type") == "doc_download", 1).otherwise(0)
        ).alias("doc_downloads_count"),
        F.max("event_timestamp").alias("last_digital_event_ts"),
    ).withColumn(
        "digital_engagement_score",
        (F.col("logins_count") * 1.0) + (F.col("doc_downloads_count") * 2.0)
    )
    
    # Retirement participation (via member_id = customer_id), normalize status before active filter.
    retirement_features_df = retirement_df.withColumn(
        "member_status_normalized",
        F.upper(F.trim(F.coalesce(F.col("member_status"), F.lit(""))))
    ).filter(
        F.col("member_status_normalized") == "ACTIVE"
    ).groupBy(
        F.col("member_id").alias("customer_id")
    ).agg(
        F.sum("contribution_amount").alias("total_retirement_contribution"),
        F.when(
            F.sum("contribution_amount") > 0, F.lit(True)
        ).otherwise(F.lit(False)).alias("has_retirement_account"),
    )
    
    # Combine features
    ml_features_df = policy_features_df.join(
        digital_features_df, ["customer_id"], "left"
    ).join(
        retirement_features_df, ["customer_id"], "left"
    )
    
    # Rename customer_id to c360_customer_id for ML context
    result_df = ml_features_df.withColumnRenamed(
        "customer_id", "c360_customer_id"
    ).withColumn(
        "gold_ingested_at", F.current_timestamp()
    ).withColumn(
        "gold_run_id", F.lit(run_id)
    )
    
    return result_df


def build_book_of_business():
    """
    G4: Fact with Unity Catalog RLS row filter on advisor_id.
    Includes disability_gap_flag and ci_cross_sell_flag.
    """
    customer_df = read_silver_source("customer.master")
    advisor_df = read_silver_source("freedom55.advisor_feed_clean")
    policy_enriched_df = read_silver_source("policy.individual_life_enriched")
    
    # Base: advisor assignments (active only)
    advisor_base_df = advisor_df.filter(
        (F.col("advisor_assignment_status") == "Active")
        | (F.col("assignment_end_date").isNull())
    ).select(
        F.col("advisor_id"),
        F.col("customer_id"),
        F.col("assignment_start_date"),
        F.col("region"),
        F.col("channel"),
        F.col("policy_number"),
    )
    
    # Join customer details
    book_df = advisor_base_df.join(
        customer_df.select(
            F.col("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("province_clean"),
        ),
        ["customer_id"],
        "left"
    )
    
    # Join policy details using the live compact canonical status values.
    policy_current_df = policy_enriched_df.withColumn(
        "policy_status_canonical_normalized",
        F.upper(F.trim(F.coalesce(F.col("policy_status_canonical"), F.lit(""))))
    ).filter(
        ~F.col("policy_status_canonical_normalized").isin("L", "C")
    ).select(
        F.col("customer_id"),
        F.col("policy_number"),
        F.col("product_type_code_canonical"),
        F.col("annualised_premium"),
        F.col("churn_risk_signal"),
        F.col("term_expiring_90d_flag"),
    )
    
    book_with_policy_df = book_df.join(
        policy_current_df,
        ["customer_id", "policy_number"],
        "left"
    )
    
    # Derived flags for cross-sell opportunities
    book_with_flags_df = book_with_policy_df.withColumn(
        "disability_gap_flag",
        F.when(
            ~F.col("product_type_code_canonical").rlike("(?i)DI|DISABILITY"),
            F.lit(True)
        ).otherwise(F.lit(False))
    ).withColumn(
        "ci_cross_sell_flag",
        F.when(
            ~F.col("product_type_code_canonical").rlike("(?i)CI|CRITICAL")
            & (F.col("term_expiring_90d_flag") == True),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    
    # Add gold metadata
    result_df = book_with_flags_df.withColumn(
        "gold_ingested_at", F.current_timestamp()
    ).withColumn(
        "gold_run_id", F.lit(run_id)
    )
    
    return result_df


def build_kpi_summary():
    """
    G5: Daily KPIs aggregate including 30d churn rate using SCD2 effective_date.
    """
    policy_clean_df = read_silver_source("policy.individual_life_clean")
    policy_with_normalized_status = policy_clean_df.withColumn(
        "policy_status_normalized",
        F.upper(F.trim(F.coalesce(F.col("policy_status_code"), F.lit(""))))
    )
    
    # Calculate 30-day churn rate using SCD2 effective_date and live status values.
    churn_window_df = policy_with_normalized_status.filter(
        F.col("effective_date").isNotNull()
    ).filter(
        F.datediff(F.current_date(), F.col("effective_date")).between(0, 30)
    ).groupBy(
        F.to_date(F.col("effective_date")).alias("kpi_date")
    ).agg(
        F.countDistinct("customer_id").alias("active_customers"),
        F.sum(
            F.when(F.col("policy_status_normalized").isin("L", "LAPSED", "C", "CANCELLED"), 1).otherwise(0)
        ).alias("churned_customers"),
    ).withColumn(
        "churn_rate_30d",
        F.when(
            F.col("active_customers") > 0,
            F.col("churned_customers") / F.col("active_customers")
        ).otherwise(F.lit(0.0))
    )
    
    # Other daily KPIs (policies issued, premiums written) on the live current-state slice.
    daily_kpis_df = policy_with_normalized_status.filter(
        F.col("policy_status_normalized").isin("ACTIVE", "PENDING", "SUSPENDED", "A", "P", "S")
    ).filter(
        F.col("issue_date").isNotNull()
    ).groupBy(
        F.to_date(F.col("issue_date")).alias("kpi_date")
    ).agg(
        F.count("*").alias("policies_issued"),
        F.sum("premium_amount").alias("total_premium_written"),
        F.sum("face_amount").alias("total_face_amount_issued"),
    )
    
    # Merge KPIs
    kpi_df = churn_window_df.join(
        daily_kpis_df, ["kpi_date"], "outer"
    )
    
    # Add gold metadata
    result_df = kpi_df.withColumn(
        "gold_ingested_at", F.current_timestamp()
    ).withColumn(
        "gold_run_id", F.lit(run_id)
    )
    
    return result_df


# ------------------------------------------------------------------------------
# 5. Execution Orchestration
# ------------------------------------------------------------------------------
ensure_gold_schema()
selected_targets = resolve_selected_targets()
result_rows = []

print(f"GOLD_EXECUTION_MODE={execution_mode}")
print(f"GOLD_SELECTED_TARGETS={', '.join(selected_targets)}")
print(f"GOLD_RUN_ID={run_id}")
print(f"GOLD_RUN_DATE={run_date or 'FULL'}")
print("="*80)

for target_name_value in selected_targets:
    target_config = TARGET_CONFIG[target_name_value]
    target_kind = target_config["kind"]
    target_write_mode = target_config["write_mode"]
    
    print(f"Processing target: {target_name_value} (kind={target_kind}, mode={target_write_mode})")
    
    if execution_mode == "PLAN":
        result_rows.append({
            "target_name": target_name_value,
            "kind": target_kind,
            "write_mode": target_write_mode,
            "row_count": None,
            "target_table": gold_table_fqn(target_name_value),
            "target_path": gold_storage_path(target_name_value),
            "status": "PLANNED",
            "message": "Execution skipped in PLAN mode",
        })
    else:
        try:
            target_df = build_target_dataframe(target_name_value)
            target_count = target_df.count()
            
            if execution_mode == "TEST":
                print(f"  Row count: {target_count}")
                preview_df = target_df.limit(5)
                display(preview_df)
                result_rows.append({
                    "target_name": target_name_value,
                    "kind": target_kind,
                    "write_mode": target_write_mode,
                    "row_count": target_count,
                    "target_table": gold_table_fqn(target_name_value),
                    "target_path": gold_storage_path(target_name_value),
                    "status": "TESTED",
                    "message": f"Preview generated with {target_count} rows",
                })
            elif execution_mode == "RUN":
                write_delta(target_df, target_name_value, target_write_mode)
                maybe_optimize(target_name_value, target_config["keys"])
                print(f"  Written {target_count} rows to {gold_table_fqn(target_name_value)}")
                result_rows.append({
                    "target_name": target_name_value,
                    "kind": target_kind,
                    "write_mode": target_write_mode,
                    "row_count": target_count,
                    "target_table": gold_table_fqn(target_name_value),
                    "target_path": gold_storage_path(target_name_value),
                    "status": "SUCCESS",
                    "message": f"Successfully written {target_count} rows",
                })
        except Exception as exc:
            import traceback
            error_message = f"{type(exc).__name__}: {str(exc)}"
            print(f"  FAILED: {error_message}")
            traceback.print_exc(limit=10)
            result_rows.append({
                "target_name": target_name_value,
                "kind": target_kind,
                "write_mode": target_write_mode,
                "row_count": None,
                "target_table": gold_table_fqn(target_name_value),
                "target_path": gold_storage_path(target_name_value),
                "status": "FAILED",
                "message": error_message,
            })

print("="*80)
print(f"GOLD_EXECUTION_COMPLETE|mode={execution_mode}|targets={len(selected_targets)}")

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

# DBTITLE 1,Gold validation checks
# ------------------------------------------------------------------------------
# 6. Gold Validation Checks
# ------------------------------------------------------------------------------
if execution_mode != "RUN":
    print(f"Gold validation skipped in {execution_mode} mode.")
else:
    validation_results = []
    
    # Check 1: All 5 gold tables exist
    all_gold_tables_exist = all(
        table_exists(gold_table_fqn(target_name))
        for target_name in TARGET_ORDER
    )
    validation_results.append({"check_name": "all_5_tables_present", "passed": all_gold_tables_exist})
    
    # Check 2: customer_360 has expected columns and rows
    if table_exists(gold_table_fqn("customer_360")):
        customer_360_df = spark.table(gold_table_fqn("customer_360"))
        expected_c360_cols = {"customer_id", "first_name", "last_name", "email_clean", "phone_clean"}
        has_c360_cols = expected_c360_cols.issubset(set(customer_360_df.columns))
        validation_results.append({"check_name": "customer_360_has_expected_cols", "passed": has_c360_cols})
        
        customer_count = customer_360_df.count()
        validation_results.append({"check_name": "customer_360_populated", "passed": customer_count > 0})
    else:
        validation_results.append({"check_name": "customer_360_has_expected_cols", "passed": False})
        validation_results.append({"check_name": "customer_360_populated", "passed": False})
    
    # Check 3: regulatory_view has expected columns
    if table_exists(gold_table_fqn("regulatory_view")):
        regulatory_df = spark.table(gold_table_fqn("regulatory_view"))
        expected_regulatory_cols = {"product_type_code_canonical", "province_clean", "gross_face_amount", "net_face_amount"}
        has_regulatory_cols = expected_regulatory_cols.issubset(set(regulatory_df.columns))
        validation_results.append({"check_name": "regulatory_view_has_expected_cols", "passed": has_regulatory_cols})
        
        regulatory_count = regulatory_df.count()
        validation_results.append({"check_name": "regulatory_view_populated", "passed": regulatory_count > 0})
    else:
        validation_results.append({"check_name": "regulatory_view_has_expected_cols", "passed": False})
        validation_results.append({"check_name": "regulatory_view_populated", "passed": False})
    
    # Check 4: ml_features has c360_customer_id, lapse_count_lifetime, reinstatement_count
    if table_exists(gold_table_fqn("ml_features")):
        ml_features_df = spark.table(gold_table_fqn("ml_features"))
        expected_ml_cols = {"c360_customer_id", "lapse_count_lifetime", "reinstatement_count"}
        has_ml_cols = expected_ml_cols.issubset(set(ml_features_df.columns))
        validation_results.append({"check_name": "ml_features_has_expected_cols", "passed": has_ml_cols})
        
        ml_count = ml_features_df.count()
        validation_results.append({"check_name": "ml_features_populated", "passed": ml_count > 0})
    else:
        validation_results.append({"check_name": "ml_features_has_expected_cols", "passed": False})
        validation_results.append({"check_name": "ml_features_populated", "passed": False})
    
    # Check 5: book_of_business has advisor_id and cross-sell flags
    if table_exists(gold_table_fqn("book_of_business")):
        book_df = spark.table(gold_table_fqn("book_of_business"))
        expected_book_cols = {"advisor_id", "customer_id", "disability_gap_flag", "ci_cross_sell_flag"}
        has_book_cols = expected_book_cols.issubset(set(book_df.columns))
        validation_results.append({"check_name": "book_of_business_has_expected_cols", "passed": has_book_cols})
        
        book_count = book_df.count()
        validation_results.append({"check_name": "book_of_business_populated", "passed": book_count > 0})
    else:
        validation_results.append({"check_name": "book_of_business_has_expected_cols", "passed": False})
        validation_results.append({"check_name": "book_of_business_populated", "passed": False})
    
    # Check 6: kpi_summary has kpi_date and churn_rate_30d
    if table_exists(gold_table_fqn("kpi_summary")):
        kpi_df = spark.table(gold_table_fqn("kpi_summary"))
        expected_kpi_cols = {"kpi_date", "churn_rate_30d"}
        has_kpi_cols = expected_kpi_cols.issubset(set(kpi_df.columns))
        validation_results.append({"check_name": "kpi_summary_has_expected_cols", "passed": has_kpi_cols})
        
        kpi_count = kpi_df.count()
        validation_results.append({"check_name": "kpi_summary_populated", "passed": kpi_count > 0})
    else:
        validation_results.append({"check_name": "kpi_summary_has_expected_cols", "passed": False})
        validation_results.append({"check_name": "kpi_summary_populated", "passed": False})
    
    # Check 7: All gold tables have gold_run_id and gold_ingested_at
    all_have_gold_metadata = True
    for target_name in TARGET_ORDER:
        if table_exists(gold_table_fqn(target_name)):
            table_df = spark.table(gold_table_fqn(target_name))
            table_cols = set(table_df.columns)
            if "gold_run_id" not in table_cols or "gold_ingested_at" not in table_cols:
                all_have_gold_metadata = False
                break
        else:
            all_have_gold_metadata = False
            break
    validation_results.append({"check_name": "all_tables_have_gold_metadata", "passed": all_have_gold_metadata})
    
    # Display validation results
    validation_schema = T.StructType([
        T.StructField("check_name", T.StringType(), True),
        T.StructField("passed", T.BooleanType(), True),
    ])
    validation_df = spark.createDataFrame(validation_results, validation_schema)
    display(validation_df)
    
    # Summary
    failed_checks = [check for check in validation_results if not check["passed"]]
    if failed_checks:
        failed_check_names = ", ".join(check["check_name"] for check in failed_checks)
        print(f"FAILED_VALIDATION_CHECKS={failed_check_names}")
        print("GOLD_EXPECTATION_STATUS=FAIL")
    else:
        print("FAILED_VALIDATION_CHECKS=NONE")
        print("GOLD_EXPECTATION_STATUS=PASS")
    
    # Sample from each gold table
    sample_results = []
    for target_name in TARGET_ORDER:
        if table_exists(gold_table_fqn(target_name)):
            table_df = spark.table(gold_table_fqn(target_name))
            sample_row = table_df.limit(1).collect()
            if sample_row:
                sample_results.append({
                    "table_name": target_name,
                    "sample_value": str(sample_row[0].asDict()),
                })
    
    if sample_results:
        sample_schema = T.StructType([
            T.StructField("table_name", T.StringType(), True),
            T.StructField("sample_value", T.StringType(), True),
        ])
        sample_df = spark.createDataFrame(sample_results, sample_schema)
        display(sample_df)
