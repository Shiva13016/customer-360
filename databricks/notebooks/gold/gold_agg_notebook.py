# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC This notebook builds the Customer 360 gold layer from silver outputs.
# MAGIC
# MAGIC Sections:
# MAGIC * Parameters and runtime controls
# MAGIC * Gold lineage map and target definitions
# MAGIC * Shared helper functions and source standardization
# MAGIC * Gold target builders for G1-G5
# MAGIC * PLAN / TEST / RUN execution orchestration
# MAGIC
# MAGIC Execution modes:
# MAGIC * `PLAN`: validate source availability, target lineage, and custom storage paths without building tables
# MAGIC * `TEST`: build selected gold outputs and preview rows without writing
# MAGIC * `RUN`: write the selected gold outputs as Delta tables in business-friendly gold ADLS folders and register them in Unity Catalog, then optionally apply row-level security to G4
# MAGIC
# MAGIC Scope in this notebook:
# MAGIC * G1 `gold.customer_360`
# MAGIC * G2 `gold.regulatory_view`
# MAGIC * G3 `gold.ml_features`
# MAGIC * G4 `gold.book_of_business`
# MAGIC * G5 `gold.kpi_summary`
# MAGIC
# MAGIC Compliance audit note:
# MAGIC * G6 `gold.pipeda_audit` is built in the dedicated [Compliance Audit Notebook](#notebook-2704371916535664)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parameters and imports
# ==============================================================================
# Notebook: gold_agg_notebook
# Purpose   : Build the Canada Life Customer 360 gold aggregation layer from
#             silver outputs into a unified medallion gold schema.
# Notes     :
#             * Follows the silver notebook PLAN / TEST / RUN pattern.
#             * G3 intentionally queries full SCD2 history from
#               policy.individual_life_clean.
#             * G4 applies Unity Catalog row-level security in RUN mode.
#             * Dedicated compliance audit logic now lives in Compliance Audit Notebook.
#             * All consumer-specific outputs are materialized as tables in the
#               single gold schema to preserve medallion layering.
#             * Gold outputs are written to business-friendly custom ADLS folders
#               instead of Unity Catalog managed __unitystorage paths.
# ==============================================================================
import json
import uuid
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

for widget_name in [
    "target_table_name",
    "execution_mode",
    "catalog_name",
    "silver_schema",
    "gold_catalog_name",
    "apply_security",
    "optimize_output",
]:
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

DEFAULT_CATALOG_NAME = "dbw_c360_canadalife"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_CATALOG_NAME = "dbw_c360_canadalife"
DEFAULT_GOLD_BASE_PATH = "abfss://gold@adlsc360canadalife.dfs.core.windows.net/gold"

widget_defaults = {
    "target_table_name": "ALL",
    "execution_mode": "PLAN",
    "catalog_name": DEFAULT_CATALOG_NAME,
    "silver_schema": DEFAULT_SILVER_SCHEMA,
    "gold_catalog_name": DEFAULT_GOLD_CATALOG_NAME,
    "apply_security": "true",
    "optimize_output": "false",
}

for widget_name, default_value in widget_defaults.items():
    dbutils.widgets.text(widget_name, default_value)

catalog_name = dbutils.widgets.get("catalog_name").strip() or DEFAULT_CATALOG_NAME
silver_schema = dbutils.widgets.get("silver_schema").strip() or DEFAULT_SILVER_SCHEMA
gold_catalog_name = dbutils.widgets.get("gold_catalog_name").strip() or DEFAULT_GOLD_CATALOG_NAME
target_table_name = dbutils.widgets.get("target_table_name").strip() or "ALL"
execution_mode = (dbutils.widgets.get("execution_mode").strip() or "PLAN").upper()
apply_security = dbutils.widgets.get("apply_security").strip().lower() == "true"
optimize_output = dbutils.widgets.get("optimize_output").strip().lower() == "true"
gold_base_path = DEFAULT_GOLD_BASE_PATH.rstrip("/")
run_id = str(uuid.uuid4())

job_context_json = dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()
job_context = json.loads(job_context_json)
job_run_id = (
    job_context.get("tags", {}).get("jobRunId")
    or job_context.get("tags", {}).get("multitaskParentRunId")
    or job_context.get("currentRunId", {}).get("id")
    or job_context.get("rootRunId", {}).get("id")
)
job_task_run_id = job_context.get("currentRunId", {}).get("id") or job_run_id

if execution_mode not in {"PLAN", "TEST", "RUN"}:
    raise ValueError("execution_mode must be one of PLAN, TEST, or RUN")

silver_catalog = catalog_name
silver_table_base = f"{silver_catalog}.{silver_schema}"


# COMMAND ----------

# DBTITLE 1,Configuration summary
# MAGIC %md
# MAGIC This section defines the gold targets, their silver dependencies, and operational expectations.
# MAGIC
# MAGIC Design choices:
# MAGIC * Gold targets are grouped by consumer use case, but all outputs are written into the single `gold` schema.
# MAGIC * Gold storage uses business-friendly ADLS paths under `abfss://gold@adlsc360canadalife.dfs.core.windows.net/gold/<table_name>/`.
# MAGIC * Current-state tables filter silver `is_current = true` where required.
# MAGIC * G3 deliberately reads full SCD2 history from `policy.individual_life_clean` for churn and lapse features.
# MAGIC * G4 prepares for Unity Catalog row filtering in `RUN` mode.
# MAGIC * G6 is intentionally separated into the dedicated Compliance Audit Notebook.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuration and target metadata
TARGET_CONFIG = {
    "G1": {
        "table_name": "customer_360",
        "schema": "gold",
        "description": "Wide current-state customer fact",
        "sources": [
            "customer.master",
            "policy.individual_life_enriched",
            "policy.disability_ci_clean",
            "group_benefits.certificate_coverage_detail",
            "group_retirement.member_clean",
            "investments.climl_clean",
            "interactions.callcentre_clean",
        ],
        "write_mode": "overwrite",
    },
    "G2": {
        "table_name": "regulatory_view",
        "schema": "gold",
        "description": "OSFI and IFRS-oriented aggregate compliance fact",
        "sources": [
            "policy.individual_life_enriched",
            "reinsurance.treaty_clean",
        ],
        "write_mode": "overwrite",
    },
    "G3": {
        "table_name": "ml_features",
        "schema": "gold",
        "description": "Feature store style dataset with full SCD2 lifecycle features",
        "sources": [
            "policy.individual_life_clean",
            "policy.individual_life_enriched",
            "policy.disability_ci_clean",
            "digital.portal_clean",
            "group_retirement.member_clean",
            "freedom55.advisor_feed_clean",
            "customer.master",
        ],
        "write_mode": "overwrite",
    },
    "G4": {
        "table_name": "book_of_business",
        "schema": "gold",
        "description": "Advisor-scoped book of business with Unity Catalog row filter",
        "sources": [
            "customer.master",
            "freedom55.advisor_feed_clean",
            "policy.individual_life_enriched",
            "policy.disability_ci_clean",
        ],
        "write_mode": "overwrite",
    },
    "G5": {
        "table_name": "kpi_summary",
        "schema": "gold",
        "description": "Daily executive KPI snapshot",
        "sources": [
            "customer.master",
            "policy.individual_life_clean",
            "policy.individual_life_enriched",
            "policy.disability_ci_clean",
            "digital.portal_clean",
            "group_benefits.certificate_coverage_detail",
            "group_retirement.member_clean",
            "investments.climl_clean",
            "interactions.callcentre_clean",
        ],
        "write_mode": "overwrite",
    },
}

TARGET_ORDER = ["G1", "G2", "G3", "G4", "G5"]

LINEAGE_MAP_ROWS = [
    ("customer.master", "G1", "gold.customer_360"),
    ("policy.individual_life_enriched", "G1", "gold.customer_360"),
    ("policy.disability_ci_clean", "G1", "gold.customer_360"),
    ("group_benefits.certificate_coverage_detail", "G1", "gold.customer_360"),
    ("group_retirement.member_clean", "G1", "gold.customer_360"),
    ("investments.climl_clean", "G1", "gold.customer_360"),
    ("interactions.callcentre_clean", "G1", "gold.customer_360"),
    ("policy.individual_life_enriched", "G2", "gold.regulatory_view"),
    ("reinsurance.treaty_clean", "G2", "gold.regulatory_view"),
    ("policy.individual_life_clean", "G3", "gold.ml_features"),
    ("digital.portal_clean", "G3", "gold.ml_features"),
    ("group_retirement.member_clean", "G3", "gold.ml_features"),
    ("freedom55.advisor_feed_clean", "G3", "gold.ml_features"),
    ("customer.master", "G3", "gold.ml_features"),
    ("customer.master", "G4", "gold.book_of_business"),
    ("freedom55.advisor_feed_clean", "G4", "gold.book_of_business"),
    ("policy.individual_life_enriched", "G4", "gold.book_of_business"),
    ("policy.disability_ci_clean", "G4", "gold.book_of_business"),
    ("customer.master", "G5", "gold.kpi_summary"),
    ("policy.individual_life_clean", "G5", "gold.kpi_summary"),
    ("policy.individual_life_enriched", "G5", "gold.kpi_summary"),
    ("policy.disability_ci_clean", "G5", "gold.kpi_summary"),
    ("digital.portal_clean", "G5", "gold.kpi_summary"),
    ("group_benefits.certificate_coverage_detail", "G5", "gold.kpi_summary"),
    ("group_retirement.member_clean", "G5", "gold.kpi_summary"),
    ("investments.climl_clean", "G5", "gold.kpi_summary"),
    ("interactions.callcentre_clean", "G5", "gold.kpi_summary"),
]

LINEAGE_MAP_DF = spark.createDataFrame(
    LINEAGE_MAP_ROWS,
    ["silver_source", "gold_target_code", "gold_target_name"],
)

DATAFRAME_CACHE = {}


def logical_to_physical(name: str) -> str:
    return name.replace(".", "_").lower()


def silver_table_fqn(logical_name: str) -> str:
    return f"{silver_catalog}.{silver_schema}.{logical_to_physical(logical_name)}"


def gold_table_fqn(target_code: str) -> str:
    target = TARGET_CONFIG[target_code]
    return f"{gold_catalog_name}.{target['schema']}.{target['table_name']}"


def gold_table_path(target_code: str) -> str:
    target = TARGET_CONFIG[target_code]
    return f"{gold_base_path}/{target['table_name']}"


# COMMAND ----------

# DBTITLE 1,Shared helpers
# MAGIC %md
# MAGIC The helper layer handles:
# MAGIC * source loading and availability checks
# MAGIC * reusable customer-, policy-, and advisor-level aggregates
# MAGIC * Delta writes and optional optimization
# MAGIC * security object creation for G4
# MAGIC * reusable orchestration for G1-G5 outputs
# MAGIC

# COMMAND ----------

# DBTITLE 1,Utility functions
def resolve_selected_targets():
    selected = target_table_name.upper()
    if selected == "ALL":
        return TARGET_ORDER
    if selected in TARGET_CONFIG:
        return [selected]
    raise ValueError(f"Unsupported target_table_name: {target_table_name}. Use ALL or one of {TARGET_ORDER}")


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def ensure_gold_schema(schema_name: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog_name}.{schema_name}")


def read_silver_table(logical_name: str) -> DataFrame:
    table_name = silver_table_fqn(logical_name)
    return spark.table(table_name)


def first_existing_column(df_or_columns, candidates: list, dtype: str = "string"):
    column_names = df_or_columns if isinstance(df_or_columns, set) else set(df_or_columns.columns)
    for candidate in candidates:
        if candidate in column_names:
            return F.col(candidate).cast(dtype)
    return F.lit(None).cast(dtype)


def current_record_filter(df: DataFrame) -> DataFrame:
    return df.filter(F.coalesce(F.col("is_current"), F.lit(True)) == True) if "is_current" in df.columns else df


def union_all(dfs):
    valid_dfs = [df for df in dfs if df is not None]
    if not valid_dfs:
        raise ValueError("union_all received no dataframes")
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), valid_dfs)


def write_delta_table(df: DataFrame, target_code: str):
    target_fqn = gold_table_fqn(target_code)
    target_path = gold_table_path(target_code)
    temp_view_name = f"tmp_{logical_to_physical(target_fqn)}_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(temp_view_name)
    try:
        if table_exists(target_fqn):
            spark.sql(f"DROP TABLE {target_fqn}")
        spark.sql(
            f"""
            CREATE TABLE {target_fqn}
            USING DELTA
            LOCATION '{target_path}'
            AS SELECT * FROM {temp_view_name}
            """
        )
    finally:
        try:
            spark.catalog.dropTempView(temp_view_name)
        except Exception:
            pass
    if optimize_output:
        spark.sql(f"OPTIMIZE {target_fqn}")


def get_cached_df(name: str, builder):
    if name not in DATAFRAME_CACHE:
        DATAFRAME_CACHE[name] = builder()
    return DATAFRAME_CACHE[name]


def build_life_agg() -> DataFrame:
    def _builder():
        life_df = current_record_filter(read_silver_table("policy.individual_life_enriched"))
        customer_key = first_existing_column(life_df, ["life_insured_id", "customer_id"], "string").alias("c360_customer_id")
        return life_df.groupBy(customer_key).agg(
            F.countDistinct("policy_number").alias("policy_count"),
            F.sum(F.coalesce(F.col("annualised_premium"), F.lit(0.0))).alias("total_annualised_life_premium"),
            F.sum(F.coalesce(F.col("face_amount"), F.lit(0.0))).alias("total_life_coverage_amount"),
            F.max(F.when(F.col("term_expiring_90d_flag") == True, F.lit(1)).otherwise(F.lit(0))).alias("term_expiring_90d_flag"),
            F.max(F.col("churn_risk_signal")).alias("churn_risk_signal"),
            F.min("issue_date").alias("first_policy_issue_date"),
        )
    return get_cached_df("life_agg", _builder)


def build_dis_ci_agg() -> DataFrame:
    def _builder():
        dis_df = current_record_filter(read_silver_table("policy.disability_ci_clean"))
        customer_key = first_existing_column(dis_df, ["life_insured_id", "customer_id"], "string").alias("c360_customer_id")
        claim_status_expr = F.upper(F.coalesce(first_existing_column(dis_df, ["claim_status", "policy_status_code"], "string"), F.lit("")))
        claim_date_expr = first_existing_column(dis_df, ["claim_date", "issue_date"], "date")
        return dis_df.groupBy(customer_key).agg(
            F.max(F.when(F.upper(F.coalesce(F.col("product_type_code"), F.lit(""))).rlike("DISABILITY|DI"), F.lit(1)).otherwise(F.lit(0))).alias("has_disability_coverage"),
            F.max(F.when(F.upper(F.coalesce(F.col("product_type_code"), F.lit(""))).rlike("CRITICAL|CI"), F.lit(1)).otherwise(F.lit(0))).alias("has_ci_coverage"),
            F.max(F.when(claim_status_expr.rlike("ACTIVE|OPEN|PENDING"), F.lit(1)).otherwise(F.lit(0))).alias("has_active_claim_flag"),
            F.count(F.when(claim_date_expr >= F.date_sub(F.current_date(), 90), 1)).alias("claim_frequency_3m"),
        )
    return get_cached_df("dis_ci_agg", _builder)


def build_group_benefits_agg() -> DataFrame:
    def _builder():
        benefits_df = read_silver_table("group_benefits.certificate_coverage_detail")
        if "status" in benefits_df.columns:
            benefits_df = benefits_df.filter(F.upper(F.col("status")) == "ACTIVE")
        member_key = first_existing_column(benefits_df, ["plan_member_id", "member_id"], "string").alias("c360_customer_id")
        plan_sponsor_expr = first_existing_column(benefits_df, ["plan_sponsor_name", "employer_name"], "string")
        coverage_expr = first_existing_column(benefits_df, ["coverage_type_code", "coverage_type", "coverage_type_codes_enrolled"], "string")
        return benefits_df.groupBy(member_key).agg(
            F.max(F.lit(1)).alias("has_group_benefits"),
            F.max(plan_sponsor_expr).alias("group_plan_sponsor_name"),
            F.collect_set(coverage_expr).alias("group_coverages_enrolled"),
        )
    return get_cached_df("group_benefits_agg", _builder)


def build_group_retirement_agg() -> DataFrame:
    def _builder():
        gr_df = current_record_filter(read_silver_table("group_retirement.member_clean"))
        member_key = first_existing_column(gr_df, ["customer_id", "member_id"], "string").alias("c360_customer_id")
        balance_expr = F.coalesce(
            first_existing_column(gr_df, ["account_balance_current", "account_balance", "transaction_amount"], "double"),
            F.lit(0.0),
        )
        contribution_expr = F.coalesce(
            first_existing_column(gr_df, ["contribution_ytd", "employee_contribution_ytd", "transaction_amount"], "double"),
            F.lit(0.0),
        )
        return gr_df.groupBy(member_key).agg(
            F.max(F.lit(1)).alias("has_group_retirement"),
            F.sum(balance_expr).alias("group_retirement_total_balance"),
            F.sum(contribution_expr).alias("group_retirement_contribution_ytd"),
        )
    return get_cached_df("group_retirement_agg", _builder)


def build_investments_agg() -> DataFrame:
    def _builder():
        inv_df = current_record_filter(read_silver_table("investments.climl_clean"))
        annuitant_key = first_existing_column(inv_df, ["annuitant_id", "customer_id"], "string").alias("c360_customer_id")
        return inv_df.groupBy(annuitant_key).agg(
            F.max(F.lit(1)).alias("has_segregated_fund"),
            F.sum(F.coalesce(first_existing_column(inv_df, ["market_value", "total_invested_assets"], "double"), F.lit(0.0))).alias("total_invested_assets"),
            F.sum(F.coalesce(first_existing_column(inv_df, ["maturity_guarantee_amount", "maturity_guarantee"], "double"), F.lit(0.0))).alias("total_maturity_guarantee"),
        )
    return get_cached_df("investments_agg", _builder)


def build_interactions_agg() -> DataFrame:
    def _builder():
        interactions_df = read_silver_table("interactions.callcentre_clean")
        customer_key = first_existing_column(interactions_df, ["c360_customer_id", "customer_id", "advisor_id"], "string").alias("c360_customer_id")
        interaction_date_expr = F.coalesce(
            first_existing_column(interactions_df, ["interaction_timestamp", "interaction_date"], "date"),
            F.to_date("_ingested_at"),
        )
        return interactions_df.groupBy(customer_key).agg(
            F.max(interaction_date_expr).alias("last_interaction_date")
        ).withColumn(
            "days_since_last_interaction",
            F.datediff(F.current_date(), F.col("last_interaction_date"))
        )
    return get_cached_df("interactions_agg", _builder)


def create_or_replace_row_filter_function():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog_name}.security")
    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {gold_catalog_name}.security.advisor_row_filter(advisor_id_col STRING)
        RETURN is_account_group_member(concat('canada_life_advisor_', advisor_id_col))
        """
    )


def apply_book_of_business_row_filter():
    create_or_replace_row_filter_function()
    spark.sql(
        f"ALTER TABLE {gold_table_fqn('G4')} SET ROW FILTER {gold_catalog_name}.security.advisor_row_filter ON (advisor_id)"
    )


# COMMAND ----------

# DBTITLE 1,Gold builders overview
# MAGIC %md
# MAGIC Each builder below corresponds to one gold target:
# MAGIC * G1 current-state customer 360 fact
# MAGIC * G2 regulatory aggregate fact with gross/net exposure logic
# MAGIC * G3 ML feature dataset with SCD2 lifecycle features
# MAGIC * G4 advisor fact with row-level security
# MAGIC * G5 daily KPI aggregate fact
# MAGIC

# COMMAND ----------

# DBTITLE 1,Gold target builders
def build_g1_customer_360() -> DataFrame:
    customer_source_df = current_record_filter(read_silver_table("customer.master"))
    customer_columns = set(customer_source_df.columns)
    customer_df = customer_source_df.select(
        F.col("customer_id").alias("c360_customer_id"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name_clean"),
        first_existing_column(customer_columns, ["date_of_birth", "dob"], "date").alias("date_of_birth"),
        first_existing_column(customer_columns, ["province_clean", "province"], "string").alias("province_clean"),
        first_existing_column(customer_columns, ["advisor_id_primary", "advisor_id"], "string").alias("advisor_id_primary"),
        first_existing_column(customer_columns, ["match_confidence"], "string").alias("match_confidence"),
    ).withColumn(
        "match_confidence",
        F.coalesce(F.col("match_confidence"), F.lit("MEDIUM")),
    )

    g1_df = (
        customer_df
        .join(build_life_agg(), ["c360_customer_id"], "left")
        .join(build_dis_ci_agg(), ["c360_customer_id"], "left")
        .join(build_group_benefits_agg(), ["c360_customer_id"], "left")
        .join(build_group_retirement_agg(), ["c360_customer_id"], "left")
        .join(build_investments_agg(), ["c360_customer_id"], "left")
        .join(build_interactions_agg(), ["c360_customer_id"], "left")
        .withColumn(
            "cross_sell_propensity_ci",
            F.when(
                (F.coalesce(F.col("has_ci_coverage"), F.lit(0)) == 0)
                & ((F.datediff(F.current_date(), F.col("date_of_birth")) / F.lit(365.0)).between(40, 60)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "cross_sell_term_conversion_flag",
            F.coalesce(F.col("term_expiring_90d_flag"), F.lit(0)),
        )
        .withColumn(
            "total_annual_premium_all_products",
            F.coalesce(F.col("total_annualised_life_premium"), F.lit(0.0))
            + F.coalesce(F.col("group_retirement_contribution_ytd"), F.lit(0.0)),
        )
        .withColumn("gold_target_code", F.lit("G1"))
        .withColumn("gold_run_id", F.lit(run_id))
        .withColumn("gold_refreshed_at", F.current_timestamp())
    )
    return g1_df


def build_g2_regulatory_view() -> DataFrame:
    life_df = current_record_filter(read_silver_table("policy.individual_life_enriched"))
    treaty_df = current_record_filter(read_silver_table("reinsurance.treaty_clean"))

    gross_df = life_df.groupBy(
        F.lit("INDIVIDUAL_LIFE").alias("product_line"),
        F.col("product_type_code_canonical").alias("product_type_code"),
        F.col("province_clean").alias("province"),
        F.col("policy_status_canonical").alias("policy_status"),
    ).agg(
        F.count("policy_number").alias("policy_count"),
        F.sum(F.coalesce(F.col("face_amount"), F.lit(0.0))).alias("gross_face_amount"),
        F.sum(F.coalesce(F.col("annualised_premium"), F.lit(0.0))).alias("gross_annualised_premium"),
        F.sum(
            F.when(F.col("issue_date") >= F.lit("2023-01-01"), F.coalesce(F.col("annualised_premium"), F.lit(0.0))).otherwise(F.lit(0.0))
        ).alias("ifrs17_premium"),
    )

    treaty_to_policy_df = life_df.select(
        F.col("policy_number").alias("life_policy_number"),
        F.col("product_type_code_canonical"),
        F.col("province_clean"),
        F.col("policy_status_canonical"),
    ).dropDuplicates(["life_policy_number"])

    ceded_df = treaty_df.alias("t").join(
        treaty_to_policy_df.alias("l"),
        F.col("t.policy_number") == F.col("l.life_policy_number"),
        "left",
    ).groupBy(
        F.lit("INDIVIDUAL_LIFE").alias("product_line"),
        F.coalesce(F.col("l.product_type_code_canonical"), F.col("t.product_type_code"), F.lit("UNKNOWN")).alias("product_type_code"),
        F.col("l.province_clean").alias("province"),
        F.coalesce(F.col("l.policy_status_canonical"), F.col("t.policy_status_code"), F.lit("UNKNOWN")).alias("policy_status"),
    ).agg(
        F.sum(F.coalesce(F.col("t.ceded_amount"), F.lit(0.0))).alias("ceded_face_amount")
    )

    return gross_df.join(ceded_df, ["product_line", "product_type_code", "province", "policy_status"], "left").withColumn(
        "ceded_face_amount", F.coalesce(F.col("ceded_face_amount"), F.lit(0.0))
    ).withColumn(
        "net_face_amount", F.col("gross_face_amount") - F.col("ceded_face_amount")
    ).withColumn(
        "gold_target_code", F.lit("G2")
    ).withColumn(
        "gold_run_id", F.lit(run_id)
    ).withColumn(
        "gold_refreshed_at", F.current_timestamp()
    )


def build_g3_ml_features() -> DataFrame:
    customer_source_df = current_record_filter(read_silver_table("customer.master"))
    customer_columns = set(customer_source_df.columns)
    customer_df = customer_source_df.select(
        F.col("customer_id").alias("c360_customer_id"),
        first_existing_column(customer_columns, ["match_confidence"], "string").alias("match_confidence"),
        first_existing_column(customer_columns, ["advisor_id_primary", "advisor_id"], "string").alias("advisor_id"),
    ).withColumn(
        "match_confidence",
        F.coalesce(F.col("match_confidence"), F.lit("MEDIUM")),
    )
    life_current_df = current_record_filter(read_silver_table("policy.individual_life_enriched"))
    life_history_df = read_silver_table("policy.individual_life_clean")
    digital_df = read_silver_table("digital.portal_clean")
    retirement_df = current_record_filter(read_silver_table("group_retirement.member_clean"))
    advisor_df = current_record_filter(read_silver_table("freedom55.advisor_feed_clean"))

    policy_features_df = life_current_df.groupBy(first_existing_column(life_current_df, ["life_insured_id", "customer_id"], "string").alias("c360_customer_id")).agg(
        F.datediff(F.current_date(), F.min("issue_date")).alias("policy_tenure_days"),
        F.sum(
            F.when(F.col("policy_status_canonical") == "INFORCE", F.coalesce(F.col("annualised_premium"), F.lit(0.0))).otherwise(F.lit(0.0))
        ).alias("total_premium_12m"),
    )

    history_features_df = life_history_df.groupBy(first_existing_column(life_history_df, ["life_insured_id", "customer_id"], "string").alias("c360_customer_id")).agg(
        F.sum(F.when(F.upper(F.coalesce(F.col("policy_status_code"), F.lit(""))) == "LAPSED", F.lit(1)).otherwise(F.lit(0))).alias("lapse_count_lifetime"),
        F.sum(F.when(F.upper(F.coalesce(F.col("policy_status_code"), F.lit(""))) == "REINSTATED", F.lit(1)).otherwise(F.lit(0))).alias("reinstatement_count"),
    )

    login_points_expr = F.sum(
        F.coalesce(
            first_existing_column(digital_df, ["logins_90d"], "double"),
            F.when(F.to_date(F.col("event_timestamp")) >= F.date_sub(F.current_date(), 90), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
    ) * F.lit(2.0)

    download_points_expr = F.sum(
        F.coalesce(
            first_existing_column(digital_df, ["doc_downloads_90d"], "double"),
            F.when(
                F.upper(F.coalesce(F.col("event_type"), F.lit(""))).like("%DOWNLOAD%"),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
    )

    digital_features_df = (
        digital_df.groupBy(first_existing_column(digital_df, ["c360_customer_id", "customer_id"], "string").alias("c360_customer_id"))
        .agg((login_points_expr + download_points_expr).alias("engagement_points"))
        .withColumn("digital_engagement_score", F.col("engagement_points") / F.lit(90.0))
        .drop("engagement_points")
    )

    retirement_features_df = retirement_df.groupBy(first_existing_column(retirement_df, ["customer_id", "member_id"], "string").alias("c360_customer_id")).agg(
        F.sum(F.coalesce(first_existing_column(retirement_df, ["account_balance_current", "account_balance", "transaction_amount"], "double"), F.lit(0.0))).alias("group_retirement_balance")
    )

    advisor_features_df = advisor_df.groupBy(F.col("advisor_id")).agg(
        F.datediff(
            F.current_date(),
            F.min(first_existing_column(advisor_df, ["advisor_start_date", "assignment_start_date", "valuation_date"], "date")),
        ).alias("advisor_tenure_days")
    )

    claim_features_df = build_dis_ci_agg().select("c360_customer_id", "claim_frequency_3m")

    return (
        customer_df
        .join(policy_features_df, ["c360_customer_id"], "left")
        .join(history_features_df, ["c360_customer_id"], "left")
        .join(claim_features_df, ["c360_customer_id"], "left")
        .join(digital_features_df, ["c360_customer_id"], "left")
        .join(retirement_features_df, ["c360_customer_id"], "left")
        .join(advisor_features_df, ["advisor_id"], "left")
        .withColumn("gold_target_code", F.lit("G3"))
        .withColumn("gold_run_id", F.lit(run_id))
        .withColumn("gold_refreshed_at", F.current_timestamp())
    )


def build_g4_book_of_business() -> DataFrame:
    customer_source_df = current_record_filter(read_silver_table("customer.master"))
    customer_columns = set(customer_source_df.columns)
    customer_df = customer_source_df.select(
        F.coalesce(
            first_existing_column(customer_columns, ["advisor_id_primary"], "string"),
            first_existing_column(customer_columns, ["advisor_id"], "string"),
        ).alias("advisor_id"),
        F.col("customer_id").alias("c360_customer_id"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name_clean"),
        first_existing_column(customer_columns, ["province_clean", "province"], "string").alias("province_clean"),
        first_existing_column(customer_columns, ["date_of_birth", "dob"], "date").alias("date_of_birth"),
    ).filter(F.col("advisor_id").isNotNull())
    advisor_source_df = current_record_filter(read_silver_table("freedom55.advisor_feed_clean"))
    advisor_columns = set(advisor_source_df.columns)
    advisor_df = advisor_source_df.select(
        "advisor_id",
        F.coalesce(
            first_existing_column(advisor_columns, ["advisor_name"], "string"),
            F.concat_ws(" ", first_existing_column(advisor_columns, ["advisor_first_name"], "string"), first_existing_column(advisor_columns, ["advisor_last_name"], "string")),
        ).alias("advisor_name"),
        F.coalesce(first_existing_column(advisor_columns, ["advisor_branch", "branch_name", "scenario_name"], "string"), F.lit("UNKNOWN")).alias("advisor_branch"),
    ).dropDuplicates(["advisor_id"])

    life_df = build_life_agg()
    dis_df = build_dis_ci_agg()

    return (
        customer_df
        .join(advisor_df, ["advisor_id"], "left")
        .join(life_df, ["c360_customer_id"], "left")
        .join(dis_df, ["c360_customer_id"], "left")
        .withColumn("advisor_name", F.coalesce(F.col("advisor_name"), F.col("advisor_id")))
        .withColumn("advisor_branch", F.coalesce(F.col("advisor_branch"), F.lit("UNKNOWN")))
        .withColumn(
            "disability_gap_flag",
            F.when(F.coalesce(F.col("has_disability_coverage"), F.lit(0)) == 0, F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "ci_cross_sell_flag",
            F.when(
                (F.coalesce(F.col("has_ci_coverage"), F.lit(0)) == 0)
                & ((F.datediff(F.current_date(), F.col("date_of_birth")) / F.lit(365.0)).between(40, 60)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("gold_target_code", F.lit("G4"))
        .withColumn("gold_run_id", F.lit(run_id))
        .withColumn("gold_refreshed_at", F.current_timestamp())
    )


def build_g5_kpi_summary() -> DataFrame:
    customer_df = current_record_filter(read_silver_table("customer.master")).select(F.col("customer_id").alias("c360_customer_id"))
    life_clean_df = read_silver_table("policy.individual_life_clean")
    life_enriched_df = current_record_filter(read_silver_table("policy.individual_life_enriched"))
    digital_df = read_silver_table("digital.portal_clean")

    first_policy_df = life_enriched_df.groupBy(first_existing_column(life_enriched_df, ["life_insured_id", "customer_id"], "string").alias("c360_customer_id")).agg(
        F.min("issue_date").alias("first_policy_date")
    )

    base_df = customer_df.join(first_policy_df, ["c360_customer_id"], "left")

    metrics_df = base_df.agg(
        F.current_date().alias("kpi_date"),
        F.countDistinct(F.when(F.col("first_policy_date") >= F.date_sub(F.current_date(), 7), F.col("c360_customer_id"))).alias("new_customers_7d"),
    )

    active_policy_count_df = life_enriched_df.agg(
        F.countDistinct(F.when(F.col("policy_status_canonical") == "INFORCE", F.col("policy_number"))).alias("active_individual_life_policies"),
        F.sum(
            F.when(
                (F.col("policy_status_canonical") == "INFORCE")
                & (F.date_trunc("month", F.col("issue_date")) == F.date_trunc("month", F.current_date())),
                F.coalesce(F.col("annualised_premium"), F.lit(0.0)),
            ).otherwise(F.lit(0.0))
        ).alias("gwp_mtd_individual_life"),
    )

    churn_df = life_clean_df.agg(
        (
            F.countDistinct(
                F.when(
                    (F.upper(F.coalesce(F.col("policy_status_code"), F.lit(""))) == "LAPSED")
                    & (F.col("effective_date") >= F.date_sub(F.current_date(), 30)),
                    F.col("policy_number"),
                )
            )
            / F.nullif(
                F.countDistinct(
                    F.when(
                        (F.col("effective_date") <= F.date_sub(F.current_date(), 30))
                        & ((F.col("expiry_date") > F.date_sub(F.current_date(), 30)) | F.col("expiry_date").isNull()),
                        F.col("policy_number"),
                    )
                ),
                F.lit(0),
            )
        ).alias("churn_rate_30d")
    )

    total_customers = customer_df.agg(F.countDistinct("c360_customer_id").alias("total_customers")).collect()[0][0]
    digital_customer_expr = first_existing_column(digital_df, ["c360_customer_id", "customer_id"], "string")
    digital_activity_date_expr = F.coalesce(first_existing_column(digital_df, ["last_portal_login"], "date"), F.to_date("event_timestamp"))
    digital_adoption_df = digital_df.agg(
        (
            F.countDistinct(
                F.when(
                    digital_activity_date_expr >= F.date_sub(F.current_date(), 90),
                    digital_customer_expr,
                )
            )
            / F.nullif(F.lit(total_customers), F.lit(0))
        ).alias("digital_adoption_rate_90d")
    )

    result_df = metrics_df.crossJoin(active_policy_count_df).crossJoin(churn_df).crossJoin(digital_adoption_df)
    return result_df.withColumn("gold_target_code", F.lit("G5")).withColumn("gold_run_id", F.lit(run_id)).withColumn("gold_refreshed_at", F.current_timestamp())


TARGET_BUILDERS = {
    "G1": build_g1_customer_360,
    "G2": build_g2_regulatory_view,
    "G3": build_g3_ml_features,
    "G4": build_g4_book_of_business,
    "G5": build_g5_kpi_summary,
}


# COMMAND ----------

# DBTITLE 1,Execution and orchestration
if job_task_run_id:
    print(f"job_task_run_id={job_task_run_id}")
if job_run_id:
    print(f"job_run_id={job_run_id}")

selected_targets = resolve_selected_targets()

for target_code in selected_targets:
    ensure_gold_schema(TARGET_CONFIG[target_code]["schema"])

result_rows = []

if execution_mode == "PLAN":
    for target_code in selected_targets:
        target = TARGET_CONFIG[target_code]
        source_details = []
        missing_sources = []
        for source_name in target["sources"]:
            if source_name.startswith("system."):
                source_details.append(f"{source_name} (system table)")
                continue
            source_fqn = silver_table_fqn(source_name)
            exists_flag = table_exists(source_fqn)
            source_details.append(f"{source_name} -> {source_fqn} (exists={exists_flag})")
            if not exists_flag:
                missing_sources.append(source_name)
        result_rows.append((
            target_code,
            gold_table_fqn(target_code),
            gold_table_path(target_code),
            target["description"],
            "; ".join(source_details),
            "READY" if not missing_sources else "MISSING_SOURCES",
            ", ".join(missing_sources),
        ))

    plan_df = spark.createDataFrame(
        result_rows,
        ["gold_target_code", "gold_table_fqn", "gold_storage_path", "description", "source_details", "status", "missing_sources"],
    )
    display(plan_df.orderBy("gold_target_code"))

elif execution_mode == "TEST":
    for target_code in selected_targets:
        target_df = TARGET_BUILDERS[target_code]()
        preview_count = target_df.limit(5).count()
        display(target_df.limit(5))
        result_rows.append((target_code, gold_table_fqn(target_code), gold_table_path(target_code), preview_count, "PREVIEWED"))

    summary_df = spark.createDataFrame(
        result_rows,
        ["gold_target_code", "gold_table_fqn", "gold_storage_path", "preview_rows", "status"],
    )
    display(summary_df.orderBy("gold_target_code"))

else:
    for target_code in selected_targets:
        target_df = TARGET_BUILDERS[target_code]()
        write_delta_table(target_df, target_code)
        if target_code == "G4" and apply_security:
            apply_book_of_business_row_filter()
        row_count = spark.table(gold_table_fqn(target_code)).count()
        result_rows.append((target_code, gold_table_fqn(target_code), gold_table_path(target_code), row_count, "WRITTEN"))

    result_df = spark.createDataFrame(
        result_rows,
        ["gold_target_code", "gold_table_fqn", "gold_storage_path", "row_count", "status"],
    )
    display(result_df.orderBy("gold_target_code"))
