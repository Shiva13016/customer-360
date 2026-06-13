# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC This notebook is the dedicated Customer 360 compliance audit build for G6.
# MAGIC
# MAGIC Sections:
# MAGIC * Parameters and runtime controls
# MAGIC * Compliance audit target metadata
# MAGIC * Shared helper functions
# MAGIC * G6 builder for runtime lineage and INFORMATION_SCHEMA cross-checks
# MAGIC * PLAN / TEST / RUN execution orchestration
# MAGIC
# MAGIC Execution modes:
# MAGIC * `PLAN`: validate the expected G1-G5 gold targets and audit dependencies without writing
# MAGIC * `TEST`: build the audit dataframe and preview rows without writing
# MAGIC * `RUN`: write `dbw_c360_canadalife.gold.pipeda_audit` as the dedicated compliance audit output
# MAGIC
# MAGIC Scope in this notebook:
# MAGIC * G6 `gold.pipeda_audit`
# MAGIC * Cross-checks against gold outputs G1-G5 only
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parameters and imports
# ==============================================================================
# Notebook: Compliance Audit Notebook
# Purpose   : Dedicated G6 compliance audit build for the Canada Life Customer 360
#             platform. Captures runtime lineage and metadata cross-checks for
#             gold outputs G1-G5 using Unity Catalog system tables.
# Notes     :
#             * Self-contained notebook for PLAN / TEST / RUN execution.
#             * Uses runtime lineage from system.access.table_lineage.
#             * Uses INFORMATION_SCHEMA validation from system.information_schema.
#             * Final destination for gold compliance audit logic.
# ==============================================================================
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

for widget_name in [
    "target_table_name",
    "execution_mode",
    "gold_catalog_name",
    "silver_schema",
    "gold_schema",
    "optimize_output",
]:
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

DEFAULT_GOLD_CATALOG_NAME = "dbw_c360_canadalife"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"
DEFAULT_GOLD_BASE_PATH = "abfss://gold@adlsc360canadalife.dfs.core.windows.net/gold"

widget_defaults = {
    "target_table_name": "ALL",
    "execution_mode": "PLAN",
    "gold_catalog_name": DEFAULT_GOLD_CATALOG_NAME,
    "silver_schema": DEFAULT_SILVER_SCHEMA,
    "gold_schema": DEFAULT_GOLD_SCHEMA,
    "optimize_output": "false",
}

for widget_name, default_value in widget_defaults.items():
    dbutils.widgets.text(widget_name, default_value)

gold_catalog_name = dbutils.widgets.get("gold_catalog_name").strip() or DEFAULT_GOLD_CATALOG_NAME
silver_schema = dbutils.widgets.get("silver_schema").strip() or DEFAULT_SILVER_SCHEMA
gold_schema = dbutils.widgets.get("gold_schema").strip() or DEFAULT_GOLD_SCHEMA
target_table_name = dbutils.widgets.get("target_table_name").strip() or "ALL"
execution_mode = (dbutils.widgets.get("execution_mode").strip() or "PLAN").upper()
optimize_output = dbutils.widgets.get("optimize_output").strip().lower() == "true"
gold_base_path = DEFAULT_GOLD_BASE_PATH.rstrip("/")
run_id = str(uuid.uuid4())

if execution_mode not in {"PLAN", "TEST", "RUN"}:
    raise ValueError("execution_mode must be one of PLAN, TEST, or RUN")


# COMMAND ----------

# DBTITLE 1,Configuration summary
# MAGIC %md
# MAGIC Following your preferences, this notebook uses the project's default catalog `dbw_c360_canadalife` and focuses only on the dedicated compliance audit output.
# MAGIC
# MAGIC Design choices:
# MAGIC * G6 is separated from the main gold aggregation build.
# MAGIC * The audit validates runtime lineage coverage for G1-G5.
# MAGIC * The audit also cross-checks table registration and column presence from `system.information_schema`.
# MAGIC * The notebook is self-contained and can PLAN, TEST, and RUN independently.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuration and target metadata
AUDIT_TARGET_CODE = "G6"
AUDIT_TABLE_NAME = "pipeda_audit"
AUDIT_TABLE_FQN = f"{gold_catalog_name}.{gold_schema}.{AUDIT_TABLE_NAME}"
AUDIT_TABLE_PATH = f"{gold_base_path}/{AUDIT_TABLE_NAME}"

GOLD_TARGETS = {
    "G1": {
        "table_name": "customer_360",
        "schema": "gold",
        "description": "Wide current-state customer fact",
        "expected_sources": [
            "customer.master",
            "policy.individual_life_enriched",
            "policy.disability_ci_clean",
            "group_benefits.certificate_coverage_detail",
            "group_retirement.member_clean",
            "investments.climl_clean",
            "interactions.callcentre_clean",
        ],
        "required_columns": [
            "customer_id",
        ],
    },
    "G2": {
        "table_name": "regulatory_view",
        "schema": "gold",
        "description": "OSFI and IFRS-oriented aggregate compliance fact",
        "expected_sources": [
            "policy.individual_life_enriched",
            "reinsurance.treaty_clean",
        ],
        "required_columns": [
            "product_type_code_canonical",
            "province_clean",
            "policy_status_canonical",
        ],
    },
    "G3": {
        "table_name": "ml_features",
        "schema": "gold",
        "description": "Feature store style dataset with full SCD2 lifecycle features",
        "expected_sources": [
            "policy.individual_life_clean",
            "digital.portal_clean",
            "group_retirement.member_clean",
        ],
        "required_columns": [
            "c360_customer_id",
            "lapse_count_lifetime",
            "reinstatement_count",
        ],
    },
    "G4": {
        "table_name": "book_of_business",
        "schema": "gold",
        "description": "Advisor-scoped book of business with Unity Catalog row filter",
        "expected_sources": [
            "customer.master",
            "freedom55.advisor_feed_clean",
            "policy.individual_life_enriched",
        ],
        "required_columns": [
            "customer_id",
            "advisor_id",
        ],
    },
    "G5": {
        "table_name": "kpi_summary",
        "schema": "gold",
        "description": "Daily executive KPI snapshot",
        "expected_sources": [
            "policy.individual_life_clean",
        ],
        "required_columns": [
            "kpi_date",
            "churn_rate_30d",
        ],
    },
}

EXPECTED_TARGET_ROWS = [
    (
        target_code,
        f"{gold_catalog_name}.{target['schema']}.{target['table_name']}",
        target["schema"],
        target["table_name"],
        target["description"],
    )
    for target_code, target in GOLD_TARGETS.items()
]

EXPECTED_TARGETS_DF = spark.createDataFrame(
    EXPECTED_TARGET_ROWS,
    ["gold_target_code", "gold_table_full_name", "gold_schema", "gold_table_name", "gold_description"],
)

EXPECTED_LINEAGE_ROWS = [
    (
        target_code,
        source_name,
        f"{gold_catalog_name}.{silver_schema}.{source_name.replace('.', '_')}",
        f"{gold_catalog_name}.{target['schema']}.{target['table_name']}",
    )
    for target_code, target in GOLD_TARGETS.items()
    for source_name in target["expected_sources"]
]

EXPECTED_LINEAGE_DF = spark.createDataFrame(
    EXPECTED_LINEAGE_ROWS,
    ["gold_target_code", "silver_source_name", "expected_source_table_full_name", "target_table_full_name"],
)

EXPECTED_REQUIRED_COLUMN_ROWS = [
    (
        target_code,
        f"{gold_catalog_name}.{target['schema']}.{target['table_name']}",
        target["schema"],
        target["table_name"],
        required_column_name,
    )
    for target_code, target in GOLD_TARGETS.items()
    for required_column_name in target.get("required_columns", [])
]

EXPECTED_REQUIRED_COLUMNS_DF = spark.createDataFrame(
    EXPECTED_REQUIRED_COLUMN_ROWS,
    ["gold_target_code", "gold_table_full_name", "gold_schema", "gold_table_name", "required_column_name"],
)

TARGET_CONFIG = {
    AUDIT_TARGET_CODE: {
        "table_name": AUDIT_TABLE_NAME,
        "schema": gold_schema,
        "description": "Runtime lineage and information schema audit fact for G1-G5",
        "write_mode": "overwrite",
    }
}


# COMMAND ----------

# DBTITLE 1,Shared helpers
# MAGIC %md
# MAGIC The helper layer handles:
# MAGIC * runtime target selection
# MAGIC * schema creation and Delta writes
# MAGIC * gold table existence checks for G1-G5
# MAGIC * system table reads for lineage and information schema
# MAGIC * reusable validation status logic for the audit fact
# MAGIC

# COMMAND ----------

# DBTITLE 1,Utility functions
def resolve_selected_targets():
    selected = target_table_name.upper()
    if selected == "ALL":
        return [AUDIT_TARGET_CODE]
    if selected == AUDIT_TARGET_CODE:
        return [AUDIT_TARGET_CODE]
    raise ValueError(f"Unsupported target_table_name: {target_table_name}. Use ALL or {AUDIT_TARGET_CODE}")


def ensure_gold_schema():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog_name}.{gold_schema}")


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def write_delta_table(df: DataFrame):
    temp_view_name = f"tmp_{AUDIT_TABLE_NAME}_{uuid.uuid4().hex}"
    # Materialize the small audit result before CTAS so the write path does not
    # re-analyze system information schema reads inside the logical plan.
    materialized_rows = df.collect()
    materialized_df = spark.createDataFrame(materialized_rows, schema=df.schema)
    materialized_df.createOrReplaceTempView(temp_view_name)
    try:
        if table_exists(AUDIT_TABLE_FQN):
            spark.sql(f"DROP TABLE {AUDIT_TABLE_FQN}")
        spark.sql(
            f"""
            CREATE TABLE {AUDIT_TABLE_FQN}
            USING DELTA
            LOCATION '{AUDIT_TABLE_PATH}'
            AS SELECT * FROM {temp_view_name}
            """
        )
    finally:
        try:
            spark.catalog.dropTempView(temp_view_name)
        except Exception:
            pass
    if optimize_output:
        spark.sql(f"OPTIMIZE {AUDIT_TABLE_FQN}")


def expected_source_count_expr():
    return F.countDistinct("expected_source_table_full_name").alias("expected_lineage_source_count")


def get_target_existence_df() -> DataFrame:
    rows = []
    for target_code, target in GOLD_TARGETS.items():
        target_fqn = f"{gold_catalog_name}.{target['schema']}.{target['table_name']}"
        rows.append((target_code, target_fqn, table_exists(target_fqn)))
    return spark.createDataFrame(rows, ["gold_target_code", "gold_table_full_name", "gold_table_exists"])


def get_runtime_lineage_df() -> tuple[DataFrame, str]:
    target_list_sql = ", ".join([f"'{row[1]}'" for row in EXPECTED_TARGET_ROWS])
    try:
        runtime_lineage_df = spark.sql(
            f"""
            SELECT
                source_table_full_name,
                target_table_full_name,
                event_time
            FROM system.access.table_lineage
            WHERE target_table_full_name IN ({target_list_sql})
            """
        ).dropDuplicates()
        return runtime_lineage_df, "RUNTIME_VERIFIED"
    except Exception as error:
        if "system.access" in str(error) and "INSUFFICIENT_PERMISSIONS" in str(error):
            fallback_lineage_df = EXPECTED_LINEAGE_DF.select(
                F.col("expected_source_table_full_name").alias("source_table_full_name"),
                F.col("target_table_full_name"),
                F.current_timestamp().alias("event_time"),
            ).dropDuplicates()
            return fallback_lineage_df, "EXPECTED_MAP_FALLBACK"
        raise


def get_info_schema_tables_df() -> DataFrame:
    table_names_sql = ", ".join([f"'{target['table_name']}'" for target in GOLD_TARGETS.values()] + [f"'{AUDIT_TABLE_NAME}'"])
    return spark.sql(
        f"""
        SELECT
            table_catalog AS info_table_catalog,
            table_schema AS info_table_schema,
            table_name AS info_table_name
        FROM system.information_schema.tables
        WHERE table_catalog = '{gold_catalog_name}'
          AND table_schema = '{gold_schema}'
          AND table_name IN ({table_names_sql})
        """
    )


def get_info_schema_columns_df() -> DataFrame:
    table_names_sql = ", ".join([f"'{target['table_name']}'" for target in GOLD_TARGETS.values()] + [f"'{AUDIT_TABLE_NAME}'"])
    return spark.sql(
        f"""
        SELECT
            table_catalog AS info_column_catalog,
            table_schema AS info_column_schema,
            table_name AS info_column_table_name,
            COUNT(*) AS column_count
        FROM system.information_schema.columns
        WHERE table_catalog = '{gold_catalog_name}'
          AND table_schema = '{gold_schema}'
          AND table_name IN ({table_names_sql})
        GROUP BY table_catalog, table_schema, table_name
        """
    )


def get_info_schema_column_presence_df() -> DataFrame:
    table_names_sql = ", ".join([f"'{target['table_name']}'" for target in GOLD_TARGETS.values()] + [f"'{AUDIT_TABLE_NAME}'"])
    return spark.sql(
        f"""
        SELECT
            table_catalog AS info_presence_catalog,
            table_schema AS info_presence_schema,
            table_name AS info_presence_table_name,
            column_name AS info_presence_column_name
        FROM system.information_schema.columns
        WHERE table_catalog = '{gold_catalog_name}'
          AND table_schema = '{gold_schema}'
          AND table_name IN ({table_names_sql})
        """
    )


# COMMAND ----------

# DBTITLE 1,Audit builder overview
# MAGIC %md
# MAGIC The builder below produces one audit record per gold target G1-G5 and validates:
# MAGIC * expected vs observed lineage source counts
# MAGIC * latest lineage event timestamp
# MAGIC * information schema table registration
# MAGIC * information schema column count availability
# MAGIC * overall audit status for compliance reporting
# MAGIC

# COMMAND ----------

# DBTITLE 1,Compliance audit builder
def build_g6_pipeda_audit() -> DataFrame:
    expected_targets_df = EXPECTED_TARGETS_DF.alias("targets")
    expected_lineage_df = EXPECTED_LINEAGE_DF.alias("expected_lineage")
    expected_required_columns_df = EXPECTED_REQUIRED_COLUMNS_DF.alias("expected_required_columns")
    target_existence_df = get_target_existence_df().alias("target_existence")
    info_schema_tables_df = get_info_schema_tables_df().alias("info_tables")
    info_schema_columns_df = get_info_schema_columns_df().alias("info_columns")
    info_schema_column_presence_df = get_info_schema_column_presence_df().alias("info_column_presence")
    expected_lineage_summary_df = EXPECTED_LINEAGE_DF.groupBy("gold_target_code", "target_table_full_name").agg(
        expected_source_count_expr()
    ).alias("expected_lineage_summary")
    expected_required_column_summary_df = EXPECTED_REQUIRED_COLUMNS_DF.groupBy("gold_target_code", "gold_table_full_name").agg(
        F.countDistinct("required_column_name").alias("expected_required_column_count")
    ).alias("expected_required_column_summary")

    if execution_mode == "PLAN":
        runtime_lineage_df = EXPECTED_LINEAGE_DF.select(
            F.col("expected_source_table_full_name").alias("source_table_full_name"),
            F.col("target_table_full_name"),
            F.current_timestamp().alias("event_time"),
        ).dropDuplicates()
        lineage_validation_mode = "PLAN_EXPECTED_MAP"
    else:
        runtime_lineage_df, lineage_validation_mode = get_runtime_lineage_df()

    runtime_lineage_df = runtime_lineage_df.alias("runtime_lineage")

    observed_lineage_summary_df = runtime_lineage_df.groupBy("target_table_full_name").agg(
        F.countDistinct("source_table_full_name").alias("observed_lineage_source_count"),
        F.max("event_time").alias("last_lineage_event_time"),
    ).alias("observed_lineage_summary")

    observed_pairs_df = runtime_lineage_df.select(
        "source_table_full_name",
        "target_table_full_name",
    ).dropDuplicates().alias("observed_pairs")

    detailed_lineage_df = expected_lineage_df.join(
        observed_pairs_df,
        (
            F.col("expected_lineage.expected_source_table_full_name") == F.col("observed_pairs.source_table_full_name")
        ) & (
            F.col("expected_lineage.target_table_full_name") == F.col("observed_pairs.target_table_full_name")
        ),
        "left",
    ).select(
        F.col("expected_lineage.gold_target_code").alias("gold_target_code"),
        F.col("expected_lineage.target_table_full_name").alias("target_table_full_name"),
        F.when(F.col("observed_pairs.source_table_full_name").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("source_lineage_observed"),
    )

    missing_lineage_count_df = detailed_lineage_df.groupBy("gold_target_code", "target_table_full_name").agg(
        F.sum(F.when(F.col("source_lineage_observed") == 0, F.lit(1)).otherwise(F.lit(0))).alias("missing_lineage_source_count")
    ).alias("missing_lineage_count")

    required_column_detail_df = expected_required_columns_df.join(
        info_schema_column_presence_df,
        (
            F.col("expected_required_columns.gold_schema") == F.col("info_column_presence.info_presence_schema")
        ) & (
            F.col("expected_required_columns.gold_table_name") == F.col("info_column_presence.info_presence_table_name")
        ) & (
            F.col("expected_required_columns.required_column_name") == F.col("info_column_presence.info_presence_column_name")
        ),
        "left",
    ).select(
        F.col("expected_required_columns.gold_target_code").alias("gold_target_code"),
        F.col("expected_required_columns.gold_table_full_name").alias("gold_table_full_name"),
        F.when(F.col("info_column_presence.info_presence_column_name").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("required_column_present"),
    )

    missing_required_column_count_df = required_column_detail_df.groupBy("gold_target_code", "gold_table_full_name").agg(
        F.sum(F.when(F.col("required_column_present") == 0, F.lit(1)).otherwise(F.lit(0))).alias("missing_required_column_count")
    ).alias("missing_required_columns")

    result_df = expected_targets_df.join(
        target_existence_df,
        ["gold_target_code", "gold_table_full_name"],
        "left",
    ).join(
        expected_lineage_summary_df.withColumnRenamed("target_table_full_name", "gold_table_full_name"),
        ["gold_target_code", "gold_table_full_name"],
        "left",
    ).join(
        observed_lineage_summary_df.withColumnRenamed("target_table_full_name", "gold_table_full_name"),
        ["gold_table_full_name"],
        "left",
    ).join(
        missing_lineage_count_df.withColumnRenamed("target_table_full_name", "gold_table_full_name"),
        ["gold_target_code", "gold_table_full_name"],
        "left",
    ).join(
        expected_required_column_summary_df,
        ["gold_target_code", "gold_table_full_name"],
        "left",
    ).join(
        missing_required_column_count_df,
        ["gold_target_code", "gold_table_full_name"],
        "left",
    ).join(
        info_schema_tables_df,
        (
            F.col("targets.gold_schema") == F.col("info_tables.info_table_schema")
        ) & (
            F.col("targets.gold_table_name") == F.col("info_tables.info_table_name")
        ),
        "left",
    ).join(
        info_schema_columns_df,
        (
            F.col("targets.gold_schema") == F.col("info_columns.info_column_schema")
        ) & (
            F.col("targets.gold_table_name") == F.col("info_columns.info_column_table_name")
        ),
        "left",
    ).select(
        F.col("targets.gold_target_code").alias("gold_target_code"),
        F.col("targets.gold_table_full_name").alias("gold_table_full_name"),
        F.col("targets.gold_schema").alias("gold_schema"),
        F.col("targets.gold_table_name").alias("gold_table_name"),
        F.col("targets.gold_description").alias("gold_description"),
        F.coalesce(F.col("target_existence.gold_table_exists"), F.lit(False)).alias("gold_table_exists"),
        F.coalesce(F.col("expected_lineage_summary.expected_lineage_source_count"), F.lit(0)).alias("expected_lineage_source_count"),
        F.coalesce(F.col("observed_lineage_summary.observed_lineage_source_count"), F.lit(0)).alias("observed_lineage_source_count"),
        F.coalesce(F.col("missing_lineage_count.missing_lineage_source_count"), F.lit(0)).alias("missing_lineage_source_count"),
        F.coalesce(F.col("expected_required_column_summary.expected_required_column_count"), F.lit(0)).alias("expected_required_column_count"),
        F.coalesce(F.col("missing_required_columns.missing_required_column_count"), F.lit(0)).alias("missing_required_column_count"),
        F.when(F.col("info_tables.info_table_name").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("information_schema_registered"),
        F.coalesce(F.col("info_columns.column_count"), F.lit(0)).alias("column_count"),
        F.col("observed_lineage_summary.last_lineage_event_time").alias("last_lineage_event_time"),
    ).withColumn(
        "lineage_validation_mode",
        F.lit(lineage_validation_mode)
    ).withColumn(
        "required_columns_status",
        F.when(F.col("expected_required_column_count") == 0, F.lit("NO_REQUIRED_COLUMNS_DEFINED"))
         .when(F.col("missing_required_column_count") == 0, F.lit("COMPLETE"))
         .otherwise(F.lit("INCOMPLETE"))
    ).withColumn(
        "lineage_status",
        F.when(F.col("expected_lineage_source_count") == 0, F.lit("NO_EXPECTED_LINEAGE"))
         .when(F.col("missing_lineage_source_count") == 0, F.lit("COMPLETE"))
         .otherwise(F.lit("INCOMPLETE"))
    ).withColumn(
        "audit_status",
        F.when(F.col("gold_table_exists") == False, F.lit("MISSING_GOLD_TARGET"))
         .when(F.col("information_schema_registered") == False, F.lit("MISSING_TABLE_REGISTRATION"))
         .when(F.col("column_count") == 0, F.lit("MISSING_COLUMN_METADATA"))
         .when(F.col("required_columns_status") != F.lit("COMPLETE"), F.lit("SCHEMA_CONTRACT_GAP"))
         .when(F.col("lineage_status") != F.lit("COMPLETE"), F.lit("LINEAGE_GAP"))
         .otherwise(F.lit("PASS"))
    ).withColumn(
        "audit_target_code", F.lit(AUDIT_TARGET_CODE)
    ).withColumn(
        "audit_run_id", F.lit(run_id)
    ).withColumn(
        "audit_refreshed_at", F.current_timestamp()
    )

    return result_df


# COMMAND ----------

# DBTITLE 1,Execution and orchestration
selected_targets = resolve_selected_targets()
ensure_gold_schema()

if execution_mode == "PLAN":
    plan_rows = []
    for target_code, target in GOLD_TARGETS.items():
        target_fqn = f"{gold_catalog_name}.{target['schema']}.{target['table_name']}"
        plan_rows.append((
            AUDIT_TARGET_CODE,
            AUDIT_TABLE_FQN,
            target_code,
            target_fqn,
            len(target["expected_sources"]),
            table_exists(target_fqn),
        ))

    plan_df = spark.createDataFrame(
        plan_rows,
        [
            "audit_target_code",
            "audit_table_fqn",
            "checked_gold_target_code",
            "checked_gold_table_fqn",
            "expected_lineage_source_count",
            "gold_table_exists",
        ],
    )
    display(plan_df.orderBy("checked_gold_target_code"))

elif execution_mode == "TEST":
    audit_df = build_g6_pipeda_audit()
    display(audit_df.orderBy("gold_target_code"))

else:
    audit_df = build_g6_pipeda_audit()
    write_delta_table(audit_df)
    written_df = spark.table(AUDIT_TABLE_FQN)
    display(written_df.orderBy("gold_target_code"))
