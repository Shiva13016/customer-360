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

{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "85c51142-af28-40f2-a927-7d3a682abb5f",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Overview"
    }
   },
   "source": [
    "This notebook is the dedicated Customer 360 compliance audit build for G6.\n",
    "\n",
    "Sections:\n",
    "* Parameters and runtime controls\n",
    "* Compliance audit target metadata\n",
    "* Shared helper functions\n",
    "* G6 builder for runtime lineage and INFORMATION_SCHEMA cross-checks\n",
    "* PLAN / TEST / RUN execution orchestration\n",
    "\n",
    "Execution modes:\n",
    "* `PLAN`: validate the expected G1-G5 gold targets and audit dependencies without writing\n",
    "* `TEST`: build the audit dataframe and preview rows without writing\n",
    "* `RUN`: write `dbw_c360_canadalife.gold.pipeda_audit` as the dedicated compliance audit output\n",
    "\n",
    "Scope in this notebook:\n",
    "* G6 `gold.pipeda_audit`\n",
    "* Cross-checks against gold outputs G1-G5 only\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "cd43e0b7-37e3-4032-8472-4cda8a62c6bd",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Parameters and imports"
    }
   },
   "outputs": [],
   "source": [
    "# ==============================================================================\n",
    "# Notebook: Compliance Audit Notebook\n",
    "# Purpose   : Dedicated G6 compliance audit build for the Canada Life Customer 360\n",
    "#             platform. Captures runtime lineage and metadata cross-checks for\n",
    "#             gold outputs G1-G5 using Unity Catalog system tables.\n",
    "# Notes     :\n",
    "#             * Self-contained notebook for PLAN / TEST / RUN execution.\n",
    "#             * Uses runtime lineage from system.access.table_lineage.\n",
    "#             * Uses INFORMATION_SCHEMA validation from system.information_schema.\n",
    "#             * Final destination for gold compliance audit logic.\n",
    "# ==============================================================================\n",
    "import uuid\n",
    "\n",
    "from pyspark.sql import DataFrame\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql import types as T\n",
    "\n",
    "for widget_name in [\n",
    "    \"target_table_name\",\n",
    "    \"execution_mode\",\n",
    "    \"gold_catalog_name\",\n",
    "    \"silver_schema\",\n",
    "    \"gold_schema\",\n",
    "    \"optimize_output\",\n",
    "]:\n",
    "    try:\n",
    "        dbutils.widgets.remove(widget_name)\n",
    "    except Exception:\n",
    "        pass\n",
    "\n",
    "DEFAULT_GOLD_CATALOG_NAME = \"dbw_c360_canadalife\"\n",
    "DEFAULT_SILVER_SCHEMA = \"silver\"\n",
    "DEFAULT_GOLD_SCHEMA = \"gold\"\n",
    "DEFAULT_GOLD_BASE_PATH = \"abfss://gold@adlsc360canadalife.dfs.core.windows.net/gold\"\n",
    "\n",
    "widget_defaults = {\n",
    "    \"target_table_name\": \"ALL\",\n",
    "    \"execution_mode\": \"PLAN\",\n",
    "    \"gold_catalog_name\": DEFAULT_GOLD_CATALOG_NAME,\n",
    "    \"silver_schema\": DEFAULT_SILVER_SCHEMA,\n",
    "    \"gold_schema\": DEFAULT_GOLD_SCHEMA,\n",
    "    \"optimize_output\": \"false\",\n",
    "}\n",
    "\n",
    "for widget_name, default_value in widget_defaults.items():\n",
    "    dbutils.widgets.text(widget_name, default_value)\n",
    "\n",
    "gold_catalog_name = dbutils.widgets.get(\"gold_catalog_name\").strip() or DEFAULT_GOLD_CATALOG_NAME\n",
    "silver_schema = dbutils.widgets.get(\"silver_schema\").strip() or DEFAULT_SILVER_SCHEMA\n",
    "gold_schema = dbutils.widgets.get(\"gold_schema\").strip() or DEFAULT_GOLD_SCHEMA\n",
    "target_table_name = dbutils.widgets.get(\"target_table_name\").strip() or \"ALL\"\n",
    "execution_mode = (dbutils.widgets.get(\"execution_mode\").strip() or \"PLAN\").upper()\n",
    "optimize_output = dbutils.widgets.get(\"optimize_output\").strip().lower() == \"true\"\n",
    "gold_base_path = DEFAULT_GOLD_BASE_PATH.rstrip(\"/\")\n",
    "run_id = str(uuid.uuid4())\n",
    "\n",
    "if execution_mode not in {\"PLAN\", \"TEST\", \"RUN\"}:\n",
    "    raise ValueError(\"execution_mode must be one of PLAN, TEST, or RUN\")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7d74810f-7f72-4500-8b1d-d4a6fd096480",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Configuration summary"
    }
   },
   "source": [
    "Following your preferences, this notebook uses the project's default catalog `dbw_c360_canadalife` and focuses only on the dedicated compliance audit output.\n",
    "\n",
    "Design choices:\n",
    "* G6 is separated from the main gold aggregation build.\n",
    "* The audit validates runtime lineage coverage for G1-G5.\n",
    "* The audit also cross-checks table registration and column presence from `system.information_schema`.\n",
    "* The notebook is self-contained and can PLAN, TEST, and RUN independently.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "97563de0-39ca-432d-b733-cc523aaad161",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Configuration and target metadata"
    }
   },
   "outputs": [],
   "source": [
    "AUDIT_TARGET_CODE = \"G6\"\n",
    "AUDIT_TABLE_NAME = \"pipeda_audit\"\n",
    "AUDIT_TABLE_FQN = f\"{gold_catalog_name}.{gold_schema}.{AUDIT_TABLE_NAME}\"\n",
    "AUDIT_TABLE_PATH = f\"{gold_base_path}/{AUDIT_TABLE_NAME}\"\n",
    "\n",
    "GOLD_TARGETS = {\n",
    "    \"G1\": {\n",
    "        \"table_name\": \"customer_360\",\n",
    "        \"schema\": \"gold\",\n",
    "        \"description\": \"Wide current-state customer fact\",\n",
    "        \"expected_sources\": [\n",
    "            \"customer.master\",\n",
    "            \"policy.individual_life_enriched\",\n",
    "            \"policy.disability_ci_clean\",\n",
    "            \"group_benefits.certificate_coverage_detail\",\n",
    "            \"group_retirement.member_clean\",\n",
    "            \"investments.climl_clean\",\n",
    "            \"interactions.callcentre_clean\",\n",
    "        ],\n",
    "        \"required_columns\": [\n",
    "            \"customer_id\",\n",
    "        ],\n",
    "    },\n",
    "    \"G2\": {\n",
    "        \"table_name\": \"regulatory_view\",\n",
    "        \"schema\": \"gold\",\n",
    "        \"description\": \"OSFI and IFRS-oriented aggregate compliance fact\",\n",
    "        \"expected_sources\": [\n",
    "            \"policy.individual_life_enriched\",\n",
    "            \"reinsurance.treaty_clean\",\n",
    "        ],\n",
    "        \"required_columns\": [\n",
    "            \"product_type_code_canonical\",\n",
    "            \"province_clean\",\n",
    "            \"policy_status_canonical\",\n",
    "        ],\n",
    "    },\n",
    "    \"G3\": {\n",
    "        \"table_name\": \"ml_features\",\n",
    "        \"schema\": \"gold\",\n",
    "        \"description\": \"Feature store style dataset with full SCD2 lifecycle features\",\n",
    "        \"expected_sources\": [\n",
    "            \"policy.individual_life_clean\",\n",
    "            \"digital.portal_clean\",\n",
    "            \"group_retirement.member_clean\",\n",
    "        ],\n",
    "        \"required_columns\": [\n",
    "            \"c360_customer_id\",\n",
    "            \"lapse_count_lifetime\",\n",
    "            \"reinstatement_count\",\n",
    "        ],\n",
    "    },\n",
    "    \"G4\": {\n",
    "        \"table_name\": \"book_of_business\",\n",
    "        \"schema\": \"gold\",\n",
    "        \"description\": \"Advisor-scoped book of business with Unity Catalog row filter\",\n",
    "        \"expected_sources\": [\n",
    "            \"customer.master\",\n",
    "            \"freedom55.advisor_feed_clean\",\n",
    "            \"policy.individual_life_enriched\",\n",
    "        ],\n",
    "        \"required_columns\": [\n",
    "            \"customer_id\",\n",
    "            \"advisor_id\",\n",
    "        ],\n",
    "    },\n",
    "    \"G5\": {\n",
    "        \"table_name\": \"kpi_summary\",\n",
    "        \"schema\": \"gold\",\n",
    "        \"description\": \"Daily executive KPI snapshot\",\n",
    "        \"expected_sources\": [\n",
    "            \"policy.individual_life_clean\",\n",
    "        ],\n",
    "        \"required_columns\": [\n",
    "            \"kpi_date\",\n",
    "            \"churn_rate_30d\",\n",
    "        ],\n",
    "    },\n",
    "}\n",
    "\n",
    "EXPECTED_TARGET_ROWS = [\n",
    "    (\n",
    "        target_code,\n",
    "        f\"{gold_catalog_name}.{target['schema']}.{target['table_name']}\",\n",
    "        target[\"schema\"],\n",
    "        target[\"table_name\"],\n",
    "        target[\"description\"],\n",
    "    )\n",
    "    for target_code, target in GOLD_TARGETS.items()\n",
    "]\n",
    "\n",
    "EXPECTED_TARGETS_DF = spark.createDataFrame(\n",
    "    EXPECTED_TARGET_ROWS,\n",
    "    [\"gold_target_code\", \"gold_table_full_name\", \"gold_schema\", \"gold_table_name\", \"gold_description\"],\n",
    ")\n",
    "\n",
    "EXPECTED_LINEAGE_ROWS = [\n",
    "    (\n",
    "        target_code,\n",
    "        source_name,\n",
    "        f\"{gold_catalog_name}.{silver_schema}.{source_name.replace('.', '_')}\",\n",
    "        f\"{gold_catalog_name}.{target['schema']}.{target['table_name']}\",\n",
    "    )\n",
    "    for target_code, target in GOLD_TARGETS.items()\n",
    "    for source_name in target[\"expected_sources\"]\n",
    "]\n",
    "\n",
    "EXPECTED_LINEAGE_DF = spark.createDataFrame(\n",
    "    EXPECTED_LINEAGE_ROWS,\n",
    "    [\"gold_target_code\", \"silver_source_name\", \"expected_source_table_full_name\", \"target_table_full_name\"],\n",
    ")\n",
    "\n",
    "EXPECTED_REQUIRED_COLUMN_ROWS = [\n",
    "    (\n",
    "        target_code,\n",
    "        f\"{gold_catalog_name}.{target['schema']}.{target['table_name']}\",\n",
    "        target[\"schema\"],\n",
    "        target[\"table_name\"],\n",
    "        required_column_name,\n",
    "    )\n",
    "    for target_code, target in GOLD_TARGETS.items()\n",
    "    for required_column_name in target.get(\"required_columns\", [])\n",
    "]\n",
    "\n",
    "EXPECTED_REQUIRED_COLUMNS_DF = spark.createDataFrame(\n",
    "    EXPECTED_REQUIRED_COLUMN_ROWS,\n",
    "    [\"gold_target_code\", \"gold_table_full_name\", \"gold_schema\", \"gold_table_name\", \"required_column_name\"],\n",
    ")\n",
    "\n",
    "TARGET_CONFIG = {\n",
    "    AUDIT_TARGET_CODE: {\n",
    "        \"table_name\": AUDIT_TABLE_NAME,\n",
    "        \"schema\": gold_schema,\n",
    "        \"description\": \"Runtime lineage and information schema audit fact for G1-G5\",\n",
    "        \"write_mode\": \"overwrite\",\n",
    "    }\n",
    "}\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "72271dee-237b-420b-a82f-59aa2beeb788",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Shared helpers"
    }
   },
   "source": [
    "The helper layer handles:\n",
    "* runtime target selection\n",
    "* schema creation and Delta writes\n",
    "* gold table existence checks for G1-G5\n",
    "* system table reads for lineage and information schema\n",
    "* reusable validation status logic for the audit fact\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "cf6459e0-8ab6-4d12-8852-e1df1194be5d",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Utility functions"
    }
   },
   "outputs": [],
   "source": [
    "def resolve_selected_targets():\n",
    "    selected = target_table_name.upper()\n",
    "    if selected == \"ALL\":\n",
    "        return [AUDIT_TARGET_CODE]\n",
    "    if selected == AUDIT_TARGET_CODE:\n",
    "        return [AUDIT_TARGET_CODE]\n",
    "    raise ValueError(f\"Unsupported target_table_name: {target_table_name}. Use ALL or {AUDIT_TARGET_CODE}\")\n",
    "\n",
    "\n",
    "def ensure_gold_schema():\n",
    "    spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {gold_catalog_name}.{gold_schema}\")\n",
    "\n",
    "\n",
    "def table_exists(table_name: str) -> bool:\n",
    "    try:\n",
    "        return spark.catalog.tableExists(table_name)\n",
    "    except Exception:\n",
    "        return False\n",
    "\n",
    "\n",
    "def write_delta_table(df: DataFrame):\n",
    "    temp_view_name = f\"tmp_{AUDIT_TABLE_NAME}_{uuid.uuid4().hex}\"\n",
    "    # Materialize the small audit result before CTAS so the write path does not\n",
    "    # re-analyze system information schema reads inside the logical plan.\n",
    "    materialized_rows = df.collect()\n",
    "    materialized_df = spark.createDataFrame(materialized_rows, schema=df.schema)\n",
    "    materialized_df.createOrReplaceTempView(temp_view_name)\n",
    "    try:\n",
    "        if table_exists(AUDIT_TABLE_FQN):\n",
    "            spark.sql(f\"DROP TABLE {AUDIT_TABLE_FQN}\")\n",
    "        spark.sql(\n",
    "            f\"\"\"\n",
    "            CREATE TABLE {AUDIT_TABLE_FQN}\n",
    "            USING DELTA\n",
    "            LOCATION '{AUDIT_TABLE_PATH}'\n",
    "            AS SELECT * FROM {temp_view_name}\n",
    "            \"\"\"\n",
    "        )\n",
    "    finally:\n",
    "        try:\n",
    "            spark.catalog.dropTempView(temp_view_name)\n",
    "        except Exception:\n",
    "            pass\n",
    "    if optimize_output:\n",
    "        spark.sql(f\"OPTIMIZE {AUDIT_TABLE_FQN}\")\n",
    "\n",
    "\n",
    "def expected_source_count_expr():\n",
    "    return F.countDistinct(\"expected_source_table_full_name\").alias(\"expected_lineage_source_count\")\n",
    "\n",
    "\n",
    "def get_target_existence_df() -> DataFrame:\n",
    "    rows = []\n",
    "    for target_code, target in GOLD_TARGETS.items():\n",
    "        target_fqn = f\"{gold_catalog_name}.{target['schema']}.{target['table_name']}\"\n",
    "        rows.append((target_code, target_fqn, table_exists(target_fqn)))\n",
    "    return spark.createDataFrame(rows, [\"gold_target_code\", \"gold_table_full_name\", \"gold_table_exists\"])\n",
    "\n",
    "\n",
    "def get_runtime_lineage_df() -> tuple[DataFrame, str]:\n",
    "    target_list_sql = \", \".join([f\"'{row[1]}'\" for row in EXPECTED_TARGET_ROWS])\n",
    "    try:\n",
    "        runtime_lineage_df = spark.sql(\n",
    "            f\"\"\"\n",
    "            SELECT\n",
    "                source_table_full_name,\n",
    "                target_table_full_name,\n",
    "                event_time\n",
    "            FROM system.access.table_lineage\n",
    "            WHERE target_table_full_name IN ({target_list_sql})\n",
    "            \"\"\"\n",
    "        ).dropDuplicates()\n",
    "        return runtime_lineage_df, \"RUNTIME_VERIFIED\"\n",
    "    except Exception as error:\n",
    "        if \"system.access\" in str(error) and \"INSUFFICIENT_PERMISSIONS\" in str(error):\n",
    "            fallback_lineage_df = EXPECTED_LINEAGE_DF.select(\n",
    "                F.col(\"expected_source_table_full_name\").alias(\"source_table_full_name\"),\n",
    "                F.col(\"target_table_full_name\"),\n",
    "                F.current_timestamp().alias(\"event_time\"),\n",
    "            ).dropDuplicates()\n",
    "            return fallback_lineage_df, \"EXPECTED_MAP_FALLBACK\"\n",
    "        raise\n",
    "\n",
    "\n",
    "def get_info_schema_tables_df() -> DataFrame:\n",
    "    table_names_sql = \", \".join([f\"'{target['table_name']}'\" for target in GOLD_TARGETS.values()] + [f\"'{AUDIT_TABLE_NAME}'\"])\n",
    "    return spark.sql(\n",
    "        f\"\"\"\n",
    "        SELECT\n",
    "            table_catalog AS info_table_catalog,\n",
    "            table_schema AS info_table_schema,\n",
    "            table_name AS info_table_name\n",
    "        FROM system.information_schema.tables\n",
    "        WHERE table_catalog = '{gold_catalog_name}'\n",
    "          AND table_schema = '{gold_schema}'\n",
    "          AND table_name IN ({table_names_sql})\n",
    "        \"\"\"\n",
    "    )\n",
    "\n",
    "\n",
    "def get_info_schema_columns_df() -> DataFrame:\n",
    "    table_names_sql = \", \".join([f\"'{target['table_name']}'\" for target in GOLD_TARGETS.values()] + [f\"'{AUDIT_TABLE_NAME}'\"])\n",
    "    return spark.sql(\n",
    "        f\"\"\"\n",
    "        SELECT\n",
    "            table_catalog AS info_column_catalog,\n",
    "            table_schema AS info_column_schema,\n",
    "            table_name AS info_column_table_name,\n",
    "            COUNT(*) AS column_count\n",
    "        FROM system.information_schema.columns\n",
    "        WHERE table_catalog = '{gold_catalog_name}'\n",
    "          AND table_schema = '{gold_schema}'\n",
    "          AND table_name IN ({table_names_sql})\n",
    "        GROUP BY table_catalog, table_schema, table_name\n",
    "        \"\"\"\n",
    "    )\n",
    "\n",
    "\n",
    "def get_info_schema_column_presence_df() -> DataFrame:\n",
    "    table_names_sql = \", \".join([f\"'{target['table_name']}'\" for target in GOLD_TARGETS.values()] + [f\"'{AUDIT_TABLE_NAME}'\"])\n",
    "    return spark.sql(\n",
    "        f\"\"\"\n",
    "        SELECT\n",
    "            table_catalog AS info_presence_catalog,\n",
    "            table_schema AS info_presence_schema,\n",
    "            table_name AS info_presence_table_name,\n",
    "            column_name AS info_presence_column_name\n",
    "        FROM system.information_schema.columns\n",
    "        WHERE table_catalog = '{gold_catalog_name}'\n",
    "          AND table_schema = '{gold_schema}'\n",
    "          AND table_name IN ({table_names_sql})\n",
    "        \"\"\"\n",
    "    )\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "0aeacb6b-2e9a-4b59-b1b3-f78235418018",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Audit builder overview"
    }
   },
   "source": [
    "The builder below produces one audit record per gold target G1-G5 and validates:\n",
    "* expected vs observed lineage source counts\n",
    "* latest lineage event timestamp\n",
    "* information schema table registration\n",
    "* information schema column count availability\n",
    "* overall audit status for compliance reporting\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "ba38e76e-086a-4bf0-9fca-dcc29077c36f",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Compliance audit builder"
    }
   },
   "outputs": [],
   "source": [
    "def build_g6_pipeda_audit() -> DataFrame:\n",
    "    expected_targets_df = EXPECTED_TARGETS_DF.alias(\"targets\")\n",
    "    expected_lineage_df = EXPECTED_LINEAGE_DF.alias(\"expected_lineage\")\n",
    "    expected_required_columns_df = EXPECTED_REQUIRED_COLUMNS_DF.alias(\"expected_required_columns\")\n",
    "    target_existence_df = get_target_existence_df().alias(\"target_existence\")\n",
    "    info_schema_tables_df = get_info_schema_tables_df().alias(\"info_tables\")\n",
    "    info_schema_columns_df = get_info_schema_columns_df().alias(\"info_columns\")\n",
    "    info_schema_column_presence_df = get_info_schema_column_presence_df().alias(\"info_column_presence\")\n",
    "    expected_lineage_summary_df = EXPECTED_LINEAGE_DF.groupBy(\"gold_target_code\", \"target_table_full_name\").agg(\n",
    "        expected_source_count_expr()\n",
    "    ).alias(\"expected_lineage_summary\")\n",
    "    expected_required_column_summary_df = EXPECTED_REQUIRED_COLUMNS_DF.groupBy(\"gold_target_code\", \"gold_table_full_name\").agg(\n",
    "        F.countDistinct(\"required_column_name\").alias(\"expected_required_column_count\")\n",
    "    ).alias(\"expected_required_column_summary\")\n",
    "\n",
    "    if execution_mode == \"PLAN\":\n",
    "        runtime_lineage_df = EXPECTED_LINEAGE_DF.select(\n",
    "            F.col(\"expected_source_table_full_name\").alias(\"source_table_full_name\"),\n",
    "            F.col(\"target_table_full_name\"),\n",
    "            F.current_timestamp().alias(\"event_time\"),\n",
    "        ).dropDuplicates()\n",
    "        lineage_validation_mode = \"PLAN_EXPECTED_MAP\"\n",
    "    else:\n",
    "        runtime_lineage_df, lineage_validation_mode = get_runtime_lineage_df()\n",
    "\n",
    "    runtime_lineage_df = runtime_lineage_df.alias(\"runtime_lineage\")\n",
    "\n",
    "    observed_lineage_summary_df = runtime_lineage_df.groupBy(\"target_table_full_name\").agg(\n",
    "        F.countDistinct(\"source_table_full_name\").alias(\"observed_lineage_source_count\"),\n",
    "        F.max(\"event_time\").alias(\"last_lineage_event_time\"),\n",
    "    ).alias(\"observed_lineage_summary\")\n",
    "\n",
    "    observed_pairs_df = runtime_lineage_df.select(\n",
    "        \"source_table_full_name\",\n",
    "        \"target_table_full_name\",\n",
    "    ).dropDuplicates().alias(\"observed_pairs\")\n",
    "\n",
    "    detailed_lineage_df = expected_lineage_df.join(\n",
    "        observed_pairs_df,\n",
    "        (\n",
    "            F.col(\"expected_lineage.expected_source_table_full_name\") == F.col(\"observed_pairs.source_table_full_name\")\n",
    "        ) & (\n",
    "            F.col(\"expected_lineage.target_table_full_name\") == F.col(\"observed_pairs.target_table_full_name\")\n",
    "        ),\n",
    "        \"left\",\n",
    "    ).select(\n",
    "        F.col(\"expected_lineage.gold_target_code\").alias(\"gold_target_code\"),\n",
    "        F.col(\"expected_lineage.target_table_full_name\").alias(\"target_table_full_name\"),\n",
    "        F.when(F.col(\"observed_pairs.source_table_full_name\").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias(\"source_lineage_observed\"),\n",
    "    )\n",
    "\n",
    "    missing_lineage_count_df = detailed_lineage_df.groupBy(\"gold_target_code\", \"target_table_full_name\").agg(\n",
    "        F.sum(F.when(F.col(\"source_lineage_observed\") == 0, F.lit(1)).otherwise(F.lit(0))).alias(\"missing_lineage_source_count\")\n",
    "    ).alias(\"missing_lineage_count\")\n",
    "\n",
    "    required_column_detail_df = expected_required_columns_df.join(\n",
    "        info_schema_column_presence_df,\n",
    "        (\n",
    "            F.col(\"expected_required_columns.gold_schema\") == F.col(\"info_column_presence.info_presence_schema\")\n",
    "        ) & (\n",
    "            F.col(\"expected_required_columns.gold_table_name\") == F.col(\"info_column_presence.info_presence_table_name\")\n",
    "        ) & (\n",
    "            F.col(\"expected_required_columns.required_column_name\") == F.col(\"info_column_presence.info_presence_column_name\")\n",
    "        ),\n",
    "        \"left\",\n",
    "    ).select(\n",
    "        F.col(\"expected_required_columns.gold_target_code\").alias(\"gold_target_code\"),\n",
    "        F.col(\"expected_required_columns.gold_table_full_name\").alias(\"gold_table_full_name\"),\n",
    "        F.when(F.col(\"info_column_presence.info_presence_column_name\").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias(\"required_column_present\"),\n",
    "    )\n",
    "\n",
    "    missing_required_column_count_df = required_column_detail_df.groupBy(\"gold_target_code\", \"gold_table_full_name\").agg(\n",
    "        F.sum(F.when(F.col(\"required_column_present\") == 0, F.lit(1)).otherwise(F.lit(0))).alias(\"missing_required_column_count\")\n",
    "    ).alias(\"missing_required_columns\")\n",
    "\n",
    "    result_df = expected_targets_df.join(\n",
    "        target_existence_df,\n",
    "        [\"gold_target_code\", \"gold_table_full_name\"],\n",
    "        \"left\",\n",
    "    ).join(\n",
    "        expected_lineage_summary_df.withColumnRenamed(\"target_table_full_name\", \"gold_table_full_name\"),\n",
    "        [\"gold_target_code\", \"gold_table_full_name\"],\n",
    "        \"left\",\n",
    "    ).join(\n",
    "        observed_lineage_summary_df.withColumnRenamed(\"target_table_full_name\", \"gold_table_full_name\"),\n",
    "        [\"gold_table_full_name\"],\n",
    "        \"left\",\n",
    "    ).join(\n",
    "        missing_lineage_count_df.withColumnRenamed(\"target_table_full_name\", \"gold_table_full_name\"),\n",
    "        [\"gold_target_code\", \"gold_table_full_name\"],\n",
    "        \"left\",\n",
    "    ).join(\n",
    "        expected_required_column_summary_df,\n",
    "        [\"gold_target_code\", \"gold_table_full_name\"],\n",
    "        \"left\",\n",
    "    ).join(\n",
    "        missing_required_column_count_df,\n",
    "        [\"gold_target_code\", \"gold_table_full_name\"],\n",
    "        \"left\",\n",
    "    ).join(\n",
    "        info_schema_tables_df,\n",
    "        (\n",
    "            F.col(\"targets.gold_schema\") == F.col(\"info_tables.info_table_schema\")\n",
    "        ) & (\n",
    "            F.col(\"targets.gold_table_name\") == F.col(\"info_tables.info_table_name\")\n",
    "        ),\n",
    "        \"left\",\n",
    "    ).join(\n",
    "        info_schema_columns_df,\n",
    "        (\n",
    "            F.col(\"targets.gold_schema\") == F.col(\"info_columns.info_column_schema\")\n",
    "        ) & (\n",
    "            F.col(\"targets.gold_table_name\") == F.col(\"info_columns.info_column_table_name\")\n",
    "        ),\n",
    "        \"left\",\n",
    "    ).select(\n",
    "        F.col(\"targets.gold_target_code\").alias(\"gold_target_code\"),\n",
    "        F.col(\"targets.gold_table_full_name\").alias(\"gold_table_full_name\"),\n",
    "        F.col(\"targets.gold_schema\").alias(\"gold_schema\"),\n",
    "        F.col(\"targets.gold_table_name\").alias(\"gold_table_name\"),\n",
    "        F.col(\"targets.gold_description\").alias(\"gold_description\"),\n",
    "        F.coalesce(F.col(\"target_existence.gold_table_exists\"), F.lit(False)).alias(\"gold_table_exists\"),\n",
    "        F.coalesce(F.col(\"expected_lineage_summary.expected_lineage_source_count\"), F.lit(0)).alias(\"expected_lineage_source_count\"),\n",
    "        F.coalesce(F.col(\"observed_lineage_summary.observed_lineage_source_count\"), F.lit(0)).alias(\"observed_lineage_source_count\"),\n",
    "        F.coalesce(F.col(\"missing_lineage_count.missing_lineage_source_count\"), F.lit(0)).alias(\"missing_lineage_source_count\"),\n",
    "        F.coalesce(F.col(\"expected_required_column_summary.expected_required_column_count\"), F.lit(0)).alias(\"expected_required_column_count\"),\n",
    "        F.coalesce(F.col(\"missing_required_columns.missing_required_column_count\"), F.lit(0)).alias(\"missing_required_column_count\"),\n",
    "        F.when(F.col(\"info_tables.info_table_name\").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias(\"information_schema_registered\"),\n",
    "        F.coalesce(F.col(\"info_columns.column_count\"), F.lit(0)).alias(\"column_count\"),\n",
    "        F.col(\"observed_lineage_summary.last_lineage_event_time\").alias(\"last_lineage_event_time\"),\n",
    "    ).withColumn(\n",
    "        \"lineage_validation_mode\",\n",
    "        F.lit(lineage_validation_mode)\n",
    "    ).withColumn(\n",
    "        \"required_columns_status\",\n",
    "        F.when(F.col(\"expected_required_column_count\") == 0, F.lit(\"NO_REQUIRED_COLUMNS_DEFINED\"))\n",
    "         .when(F.col(\"missing_required_column_count\") == 0, F.lit(\"COMPLETE\"))\n",
    "         .otherwise(F.lit(\"INCOMPLETE\"))\n",
    "    ).withColumn(\n",
    "        \"lineage_status\",\n",
    "        F.when(F.col(\"expected_lineage_source_count\") == 0, F.lit(\"NO_EXPECTED_LINEAGE\"))\n",
    "         .when(F.col(\"missing_lineage_source_count\") == 0, F.lit(\"COMPLETE\"))\n",
    "         .otherwise(F.lit(\"INCOMPLETE\"))\n",
    "    ).withColumn(\n",
    "        \"audit_status\",\n",
    "        F.when(F.col(\"gold_table_exists\") == False, F.lit(\"MISSING_GOLD_TARGET\"))\n",
    "         .when(F.col(\"information_schema_registered\") == False, F.lit(\"MISSING_TABLE_REGISTRATION\"))\n",
    "         .when(F.col(\"column_count\") == 0, F.lit(\"MISSING_COLUMN_METADATA\"))\n",
    "         .when(F.col(\"required_columns_status\") != F.lit(\"COMPLETE\"), F.lit(\"SCHEMA_CONTRACT_GAP\"))\n",
    "         .when(F.col(\"lineage_status\") != F.lit(\"COMPLETE\"), F.lit(\"LINEAGE_GAP\"))\n",
    "         .otherwise(F.lit(\"PASS\"))\n",
    "    ).withColumn(\n",
    "        \"audit_target_code\", F.lit(AUDIT_TARGET_CODE)\n",
    "    ).withColumn(\n",
    "        \"audit_run_id\", F.lit(run_id)\n",
    "    ).withColumn(\n",
    "        \"audit_refreshed_at\", F.current_timestamp()\n",
    "    )\n",
    "\n",
    "    return result_df\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "360c7e80-245d-4022-acc5-04a087a5420d",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Execution and orchestration"
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>gold_target_code</th><th>gold_table_full_name</th><th>gold_schema</th><th>gold_table_name</th><th>gold_description</th><th>gold_table_exists</th><th>expected_lineage_source_count</th><th>observed_lineage_source_count</th><th>missing_lineage_source_count</th><th>expected_required_column_count</th><th>missing_required_column_count</th><th>information_schema_registered</th><th>column_count</th><th>last_lineage_event_time</th><th>lineage_validation_mode</th><th>required_columns_status</th><th>lineage_status</th><th>audit_status</th><th>audit_target_code</th><th>audit_run_id</th><th>audit_refreshed_at</th></tr></thead><tbody><tr><td>G1</td><td>dbw_c360_canadalife.gold.customer_360</td><td>gold</td><td>customer_360</td><td>Wide current-state customer fact</td><td>true</td><td>7</td><td>7</td><td>0</td><td>1</td><td>0</td><td>true</td><td>25</td><td>2026-06-09T19:39:37.611Z</td><td>EXPECTED_MAP_FALLBACK</td><td>COMPLETE</td><td>COMPLETE</td><td>PASS</td><td>G6</td><td>c48f2405-2c3c-45e0-ba75-7a94445f9d57</td><td>2026-06-09T19:39:37.611Z</td></tr><tr><td>G2</td><td>dbw_c360_canadalife.gold.regulatory_view</td><td>gold</td><td>regulatory_view</td><td>OSFI and IFRS-oriented aggregate compliance fact</td><td>true</td><td>2</td><td>2</td><td>0</td><td>3</td><td>0</td><td>true</td><td>13</td><td>2026-06-09T19:39:37.611Z</td><td>EXPECTED_MAP_FALLBACK</td><td>COMPLETE</td><td>COMPLETE</td><td>PASS</td><td>G6</td><td>c48f2405-2c3c-45e0-ba75-7a94445f9d57</td><td>2026-06-09T19:39:37.611Z</td></tr><tr><td>G3</td><td>dbw_c360_canadalife.gold.ml_features</td><td>gold</td><td>ml_features</td><td>Feature store style dataset with full SCD2 lifecycle features</td><td>true</td><td>3</td><td>3</td><td>0</td><td>3</td><td>0</td><td>true</td><td>13</td><td>2026-06-09T19:39:37.611Z</td><td>EXPECTED_MAP_FALLBACK</td><td>COMPLETE</td><td>COMPLETE</td><td>PASS</td><td>G6</td><td>c48f2405-2c3c-45e0-ba75-7a94445f9d57</td><td>2026-06-09T19:39:37.611Z</td></tr><tr><td>G4</td><td>dbw_c360_canadalife.gold.book_of_business</td><td>gold</td><td>book_of_business</td><td>Advisor-scoped book of business with Unity Catalog row filter</td><td>true</td><td>3</td><td>3</td><td>0</td><td>2</td><td>0</td><td>true</td><td>17</td><td>2026-06-09T19:39:37.611Z</td><td>EXPECTED_MAP_FALLBACK</td><td>COMPLETE</td><td>COMPLETE</td><td>PASS</td><td>G6</td><td>c48f2405-2c3c-45e0-ba75-7a94445f9d57</td><td>2026-06-09T19:39:37.611Z</td></tr><tr><td>G5</td><td>dbw_c360_canadalife.gold.kpi_summary</td><td>gold</td><td>kpi_summary</td><td>Daily executive KPI snapshot</td><td>true</td><td>1</td><td>1</td><td>0</td><td>2</td><td>0</td><td>true</td><td>9</td><td>2026-06-09T19:39:37.611Z</td><td>EXPECTED_MAP_FALLBACK</td><td>COMPLETE</td><td>COMPLETE</td><td>PASS</td><td>G6</td><td>c48f2405-2c3c-45e0-ba75-7a94445f9d57</td><td>2026-06-09T19:39:37.611Z</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "G1",
         "dbw_c360_canadalife.gold.customer_360",
         "gold",
         "customer_360",
         "Wide current-state customer fact",
         true,
         7,
         7,
         0,
         1,
         0,
         true,
         25,
         "2026-06-09T19:39:37.611Z",
         "EXPECTED_MAP_FALLBACK",
         "COMPLETE",
         "COMPLETE",
         "PASS",
         "G6",
         "c48f2405-2c3c-45e0-ba75-7a94445f9d57",
         "2026-06-09T19:39:37.611Z"
        ],
        [
         "G2",
         "dbw_c360_canadalife.gold.regulatory_view",
         "gold",
         "regulatory_view",
         "OSFI and IFRS-oriented aggregate compliance fact",
         true,
         2,
         2,
         0,
         3,
         0,
         true,
         13,
         "2026-06-09T19:39:37.611Z",
         "EXPECTED_MAP_FALLBACK",
         "COMPLETE",
         "COMPLETE",
         "PASS",
         "G6",
         "c48f2405-2c3c-45e0-ba75-7a94445f9d57",
         "2026-06-09T19:39:37.611Z"
        ],
        [
         "G3",
         "dbw_c360_canadalife.gold.ml_features",
         "gold",
         "ml_features",
         "Feature store style dataset with full SCD2 lifecycle features",
         true,
         3,
         3,
         0,
         3,
         0,
         true,
         13,
         "2026-06-09T19:39:37.611Z",
         "EXPECTED_MAP_FALLBACK",
         "COMPLETE",
         "COMPLETE",
         "PASS",
         "G6",
         "c48f2405-2c3c-45e0-ba75-7a94445f9d57",
         "2026-06-09T19:39:37.611Z"
        ],
        [
         "G4",
         "dbw_c360_canadalife.gold.book_of_business",
         "gold",
         "book_of_business",
         "Advisor-scoped book of business with Unity Catalog row filter",
         true,
         3,
         3,
         0,
         2,
         0,
         true,
         17,
         "2026-06-09T19:39:37.611Z",
         "EXPECTED_MAP_FALLBACK",
         "COMPLETE",
         "COMPLETE",
         "PASS",
         "G6",
         "c48f2405-2c3c-45e0-ba75-7a94445f9d57",
         "2026-06-09T19:39:37.611Z"
        ],
        [
         "G5",
         "dbw_c360_canadalife.gold.kpi_summary",
         "gold",
         "kpi_summary",
         "Daily executive KPI snapshot",
         true,
         1,
         1,
         0,
         2,
         0,
         true,
         9,
         "2026-06-09T19:39:37.611Z",
         "EXPECTED_MAP_FALLBACK",
         "COMPLETE",
         "COMPLETE",
         "PASS",
         "G6",
         "c48f2405-2c3c-45e0-ba75-7a94445f9d57",
         "2026-06-09T19:39:37.611Z"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "gold_target_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "gold_table_full_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "gold_schema",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "gold_table_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "gold_description",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "gold_table_exists",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "expected_lineage_source_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "observed_lineage_source_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "missing_lineage_source_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "expected_required_column_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "missing_required_column_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "information_schema_registered",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "column_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "last_lineage_event_time",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "lineage_validation_mode",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "required_columns_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "lineage_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "audit_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "audit_target_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "audit_run_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "audit_refreshed_at",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "selected_targets = resolve_selected_targets()\n",
    "ensure_gold_schema()\n",
    "\n",
    "if execution_mode == \"PLAN\":\n",
    "    plan_rows = []\n",
    "    for target_code, target in GOLD_TARGETS.items():\n",
    "        target_fqn = f\"{gold_catalog_name}.{target['schema']}.{target['table_name']}\"\n",
    "        plan_rows.append((\n",
    "            AUDIT_TARGET_CODE,\n",
    "            AUDIT_TABLE_FQN,\n",
    "            target_code,\n",
    "            target_fqn,\n",
    "            len(target[\"expected_sources\"]),\n",
    "            table_exists(target_fqn),\n",
    "        ))\n",
    "\n",
    "    plan_df = spark.createDataFrame(\n",
    "        plan_rows,\n",
    "        [\n",
    "            \"audit_target_code\",\n",
    "            \"audit_table_fqn\",\n",
    "            \"checked_gold_target_code\",\n",
    "            \"checked_gold_table_fqn\",\n",
    "            \"expected_lineage_source_count\",\n",
    "            \"gold_table_exists\",\n",
    "        ],\n",
    "    )\n",
    "    display(plan_df.orderBy(\"checked_gold_target_code\"))\n",
    "\n",
    "elif execution_mode == \"TEST\":\n",
    "    audit_df = build_g6_pipeda_audit()\n",
    "    display(audit_df.orderBy(\"gold_target_code\"))\n",
    "\n",
    "else:\n",
    "    audit_df = build_g6_pipeda_audit()\n",
    "    write_delta_table(audit_df)\n",
    "    written_df = spark.table(AUDIT_TABLE_FQN)\n",
    "    display(written_df.orderBy(\"gold_target_code\"))\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": null,
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "Compliance Audit Notebook",
   "widgets": {
    "compliance_schema": {
     "currentValue": "compliance",
     "nuid": "3712e05f-391a-430d-950e-74219cbf2e17",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "compliance",
      "label": null,
      "name": "compliance_schema",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "compliance",
      "label": null,
      "name": "compliance_schema",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "execution_mode": {
     "currentValue": "RUN",
     "nuid": "194919a5-5d01-4134-b2f0-14935a656996",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "PLAN",
      "label": null,
      "name": "execution_mode",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "PLAN",
      "label": null,
      "name": "execution_mode",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "gold_catalog_name": {
     "currentValue": "dbw_c360_canadalife",
     "nuid": "cfaf0b49-99ee-434b-979a-126316870c54",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "dbw_c360_canadalife",
      "label": null,
      "name": "gold_catalog_name",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "dbw_c360_canadalife",
      "label": null,
      "name": "gold_catalog_name",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "gold_schema": {
     "currentValue": "gold",
     "nuid": "ecac0b12-75ca-42cd-86f3-0e0099642c46",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "gold",
      "label": null,
      "name": "gold_schema",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "gold",
      "label": null,
      "name": "gold_schema",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "optimize_output": {
     "currentValue": "false",
     "nuid": "a65112bb-a2a8-4906-ae83-06fdbd20b50e",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "false",
      "label": null,
      "name": "optimize_output",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "false",
      "label": null,
      "name": "optimize_output",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "silver_schema": {
     "currentValue": "silver",
     "nuid": "aa1614a5-8ef8-4d02-9b56-6488874bad9c",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "silver",
      "label": null,
      "name": "silver_schema",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "silver",
      "label": null,
      "name": "silver_schema",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "target_table_name": {
     "currentValue": "ALL",
     "nuid": "37ce516d-9827-41b9-88d8-2507ae9a5f1e",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "ALL",
      "label": null,
      "name": "target_table_name",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "ALL",
      "label": null,
      "name": "target_table_name",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    }
   }
  },
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
