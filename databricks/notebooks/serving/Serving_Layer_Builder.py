# Databricks notebook source
# DBTITLE 1,Canada Life C360 Serving Layer Builder
# MAGIC %md
# MAGIC # Canada Life C360 Serving Layer Builder
# MAGIC
# MAGIC ## Purpose
# MAGIC Builds the `dbw_c360_canadalife.serving` schema with dimensional and fact tables optimized for BI consumption:
# MAGIC
# MAGIC * **dim_customer**: Customer dimension (one row per customer, current state)
# MAGIC * **fact_kpi_daily**: Daily KPI fact from gold.kpi_summary
# MAGIC * **fact_advisor_book**: Advisor book fact from gold.book_of_business
# MAGIC * **fact_interactions**: Call centre interactions fact from silver.interactions_callcentre_clean
# MAGIC
# MAGIC ## Architecture
# MAGIC * **Source**: Gold layer (`customer_360`, `ml_features`, `kpi_summary`, `book_of_business`) + Silver layer (`interactions_callcentre_clean`)
# MAGIC * **Target**: `dbw_c360_canadalife.serving` schema
# MAGIC * **Write Mode**: Overwrite (full refresh)
# MAGIC * **Metadata**: `serving_ingested_at`, `serving_run_id`
# MAGIC
# MAGIC ## Execution Modes
# MAGIC * **PLAN**: Show execution plan without creating tables
# MAGIC * **TEST**: Create tables, show sample output, no production write
# MAGIC * **RUN**: Full production execution
# MAGIC
# MAGIC ## Parameters
# MAGIC * `execution_mode`: PLAN / TEST / RUN
# MAGIC * `catalog_name`: Unity Catalog name (default: dbw_c360_canadalife)
# MAGIC * `target_table_name`: Table to build (default: ALL)
# MAGIC
# MAGIC ---
# MAGIC **Recommended first run**: `dim_customer` and `fact_kpi_daily` (cleanest sources)

# COMMAND ----------

# DBTITLE 1,Imports and Setup
from pyspark.sql import functions as F
import json
import uuid

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.dropdown("execution_mode", "PLAN", ["PLAN", "TEST", "RUN"], "Execution Mode")
dbutils.widgets.text("catalog_name", "dbw_c360_canadalife", "Catalog Name")
dbutils.widgets.text("target_table_name", "ALL", "Target Table Name")

execution_mode = dbutils.widgets.get("execution_mode")
catalog_name = dbutils.widgets.get("catalog_name")
target_table_name = dbutils.widgets.get("target_table_name").upper()

serving_schema = f"{catalog_name}.serving"
serving_run_id = str(uuid.uuid4())
serving_ingested_at = F.current_timestamp()

print(f"Execution Mode: {execution_mode}")
print(f"Catalog: {catalog_name}")
print(f"Serving Schema: {serving_schema}")
print(f"Target Table: {target_table_name}")
print(f"Run ID: {serving_run_id}")

# COMMAND ----------

# DBTITLE 1,Create Serving Schema
if execution_mode in ["TEST", "RUN"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {serving_schema}")
    print(f"â Schema {serving_schema} ready")
else:
    print(f"[PLAN] Would create schema: {serving_schema}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def add_serving_metadata(df):
    """Add serving layer metadata columns"""
    return df.withColumns({
        "serving_ingested_at": serving_ingested_at,
        "serving_run_id": F.lit(serving_run_id)
    })

table_results = []


def write_serving_table(df, table_name, description=""):
    """Write serving table with metadata and execution mode handling"""
    full_table_name = f"{serving_schema}.{table_name}"

    # Add metadata
    df_with_meta = add_serving_metadata(df)

    if execution_mode == "PLAN":
        row_count = df_with_meta.count()
        print(f"\n[PLAN] {full_table_name}")
        print(f"  Description: {description}")
        print(f"  Row count estimate: {row_count:,}")
        print("  Schema:")
        df_with_meta.printSchema()
        table_results.append({
            "table_name": table_name,
            "full_table_name": full_table_name,
            "mode": execution_mode,
            "status": "planned",
            "row_count": row_count,
            "description": description
        })
        return None

    if execution_mode == "TEST":
        row_count = df_with_meta.count()
        print(f"\n[TEST] {full_table_name}")
        print(f"  Description: {description}")
        print(f"  Row count: {row_count:,}")
        print("  Sample (5 rows):")
        display(df_with_meta.limit(5))
        table_results.append({
            "table_name": table_name,
            "full_table_name": full_table_name,
            "mode": execution_mode,
            "status": "tested",
            "row_count": row_count,
            "description": description
        })
        return df_with_meta

    if execution_mode == "RUN":
        print(f"\n[RUN] Writing {full_table_name}...")
        df_with_meta.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)

        row_count = spark.table(full_table_name).count()
        print(f"  â Written: {row_count:,} rows")
        print(f"  Table: {full_table_name}")
        table_results.append({
            "table_name": table_name,
            "full_table_name": full_table_name,
            "mode": execution_mode,
            "status": "written",
            "row_count": row_count,
            "description": description
        })
        return df_with_meta

    raise ValueError(f"Unsupported execution_mode: {execution_mode}")

def should_build(table_name):
    """Check if table should be built based on target_table_name parameter"""
    return target_table_name == "ALL" or target_table_name == table_name.upper()

# COMMAND ----------

# DBTITLE 1,dim_customer Builder
# MAGIC %md
# MAGIC ## dim_customer: Customer Dimension
# MAGIC
# MAGIC **Source**: `gold.customer_360` + `gold.ml_features`  
# MAGIC **Grain**: One row per customer (current state)  
# MAGIC **Join**: `customer_360.customer_id = ml_features.c360_customer_id`

# COMMAND ----------

# DBTITLE 1,fact_interactions Builder
# MAGIC %md
# MAGIC ## fact_interactions: Call Centre Interactions Fact
# MAGIC
# MAGIC **Source**: `silver.interactions_callcentre_clean`  
# MAGIC **Grain**: One row per interaction

# COMMAND ----------

# DBTITLE 1,Build fact_interactions
if should_build("fact_interactions"):
    # Read silver source
    interactions = spark.table(f"{catalog_name}.silver.interactions_callcentre_clean")
    
    # Build fact
    fact_interactions = interactions.select(
        "interaction_id",
        "customer_id",
        "agent_id",
        "call_start_ts",
        "call_end_ts",
        "channel",
        "issue_type",
        "interaction_status",
        "call_duration_minutes"
    )
    
    write_serving_table(
        fact_interactions,
        "fact_interactions",
        "Call centre interactions fact from silver.interactions_callcentre_clean"
    )
else:
    print("[SKIP] fact_interactions")

# COMMAND ----------

# DBTITLE 1,fact_advisor_book Builder
# MAGIC %md
# MAGIC ## fact_advisor_book: Advisor Book Fact
# MAGIC
# MAGIC **Source**: `gold.book_of_business`  
# MAGIC **Grain**: One row per customer-policy-advisor assignment

# COMMAND ----------

# DBTITLE 1,Build fact_advisor_book
if should_build("fact_advisor_book"):
    # Read gold source
    book_of_business = spark.table(f"{catalog_name}.gold.book_of_business")
    
    # Build fact
    fact_advisor_book = book_of_business.select(
        "customer_id",
        "policy_number",
        "advisor_id",
        "assignment_start_date",
        "region",
        "channel",
        "product_type_code_canonical",
        "annualised_premium",
        "churn_risk_signal",
        "term_expiring_90d_flag",
        "disability_gap_flag",
        "ci_cross_sell_flag"
    )
    
    write_serving_table(
        fact_advisor_book,
        "fact_advisor_book",
        "Advisor book fact from gold.book_of_business"
    )
else:
    print("[SKIP] fact_advisor_book")

# COMMAND ----------

# DBTITLE 1,fact_kpi_daily Builder
# MAGIC %md
# MAGIC ## fact_kpi_daily: Daily KPI Fact
# MAGIC
# MAGIC **Source**: `gold.kpi_summary`  
# MAGIC **Grain**: One row per date  
# MAGIC **Note**: This is already a clean daily grain fact table

# COMMAND ----------

# DBTITLE 1,Build fact_kpi_daily
if should_build("fact_kpi_daily"):
    # Read gold source
    kpi_summary = spark.table(f"{catalog_name}.gold.kpi_summary")
    
    # Build fact (pass-through with metadata)
    fact_kpi_daily = kpi_summary.select(
        "kpi_date",
        "active_customers",
        "churned_customers",
        "churn_rate_30d",
        "policies_issued",
        "total_premium_written",
        "total_face_amount_issued"
    )
    
    write_serving_table(
        fact_kpi_daily,
        "fact_kpi_daily",
        "Daily KPI fact from gold.kpi_summary"
    )
else:
    print("[SKIP] fact_kpi_daily")

# COMMAND ----------

# DBTITLE 1,Build dim_customer
if should_build("dim_customer"):
    # Read gold sources
    c360 = spark.table(f"{catalog_name}.gold.customer_360")
    ml_features = spark.table(f"{catalog_name}.gold.ml_features")
    
    # Build dimension
    dim_customer = c360.select(
        "customer_id",
        "first_name",
        "last_name",
        "email_clean",
        "phone_clean",
        "province_clean",
        "postal_code_clean",
        "channel",
        "advisor_id",
        "first_policy_date",
        "life_policy_count",
        "total_life_face_amount",
        "total_life_premium",
        "has_term_expiring_90d",
        "has_disability_coverage",
        "has_ci_coverage",
        "group_benefits_plan_count",
        "total_retirement_contribution",
        "total_investment_market_value",
        "last_callcentre_contact",
        "callcentre_interaction_count",
        "cross_sell_propensity_ci",
        "cross_sell_term_conversion_flag"
    ).join(
        ml_features.select(
            F.col("c360_customer_id").alias("customer_id"),
            "digital_engagement_score",
            "logins_count",
            "doc_downloads_count",
            "has_retirement_account"
        ),
        on="customer_id",
        how="left"
    )
    
    write_serving_table(
        dim_customer,
        "dim_customer",
        "Customer dimension combining customer_360 and ml_features"
    )
else:
    print("[SKIP] dim_customer")

# COMMAND ----------

# DBTITLE 1,Execution Summary
# MAGIC %md
# MAGIC ## Execution Summary
# MAGIC
# MAGIC All serving layer tables have been processed based on the selected execution mode and target table filter.

# COMMAND ----------

# DBTITLE 1,Completion Log
print("="*60)
print("Serving Layer Build Complete")
print("="*60)
print(f"Execution Mode: {execution_mode}")
print(f"Catalog: {catalog_name}")
print(f"Schema: {serving_schema}")
print(f"Target Table Filter: {target_table_name}")
print(f"Run ID: {serving_run_id}")
print("="*60)

if table_results:
    job_summary_df = spark.createDataFrame(table_results)
    print("\nProcessed tables summary:")
    display(job_summary_df)
    print("\nJOB_SUMMARY_JSON=" + json.dumps(table_results, sort_keys=True))

if execution_mode == "RUN":
    print("\nServing tables created:")
    serving_tables = spark.sql(f"SHOW TABLES IN {serving_schema}").filter("isTemporary = false")
    display(serving_tables)
