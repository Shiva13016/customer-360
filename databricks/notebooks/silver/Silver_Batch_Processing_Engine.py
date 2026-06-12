# Databricks notebook source
# MAGIC %md
# MAGIC # Silver_Batch_Processing_Engine
# MAGIC
# MAGIC **Layer:** Silver
# MAGIC **Purpose:** Transforms Bronze data into Silver layer (cleansed, standardised).
# MAGIC
# MAGIC **Source path:** `/Users/shivakumaryallanti5@gmail.com/project customer 360/Silver_Batch_Processing_Engine`

# COMMAND ----------

print("Silver Batch Processing Engine - starting...")

# COMMAND ----------


{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "513b4e95-7562-45a4-a2d5-38b7839cf962",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Overview"
    }
   },
   "source": [
    "This notebook builds the Canada Life Customer 360 silver layer from bronze sources.\n",
    "\n",
    "Sections:\n",
    "* Parameters and imports\n",
    "* Configuration and target metadata\n",
    "* Shared utility functions\n",
    "* Source normalization helpers\n",
    "* Target builders\n",
    "* Execution orchestration\n",
    "\n",
    "Execution modes:\n",
    "* `PLAN` inspects configured targets without reading full outputs\n",
    "* `TEST` builds each target and previews rows without writing\n",
    "* `RUN` builds and writes silver outputs to Unity Catalog and ADLS\n",
    "\n",
    "Active runtime widgets:\n",
    "* `target_table_name`: choose one configured silver target to run, or use `ALL` to run the full silver pipeline\n",
    "* `run_date`: optional incremental date filter applied to bronze inputs using `processing_date` or `ingestion_timestamp`\n",
    "* `execution_mode`: controls whether the notebook plans, tests, or writes outputs\n",
    "* `optimize_output`: when `true`, runs `OPTIMIZE` on written silver Delta tables in `RUN` mode\n",
    "* `dq_threshold_pct`: maximum allowed null percentage for required columns in data quality gates\n",
    "\n",
    "Why these widgets remain:\n",
    "* They support production control, selective reruns, and troubleshooting\n",
    "* They do not control notebook dependencies; upstream bronze task completion is handled by the job DAG\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d45d93dd-c204-4d9c-a8fd-181968a27e9e",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Overview"
    }
   },
   "source": [
    "This notebook builds the Canada Life Customer 360 silver layer from bronze sources.\n",
    "\n",
    "Sections:\n",
    "* Parameters and imports\n",
    "* Configuration and target metadata\n",
    "* Shared utility functions\n",
    "* Source normalization helpers\n",
    "* Target builders\n",
    "* Execution orchestration\n",
    "\n",
    "Execution modes:\n",
    "* `PLAN` inspects configured targets without reading full outputs\n",
    "* `TEST` builds each target and previews rows without writing\n",
    "* `RUN` builds and writes silver outputs to Unity Catalog and ADLS\n",
    "\n",
    "Active runtime widgets:\n",
    "* `target_table_name`: choose one configured silver target to run, or use `ALL` to run the full silver pipeline\n",
    "* `run_date`: optional incremental date filter applied to bronze inputs using `processing_date` or `ingestion_timestamp`\n",
    "* `execution_mode`: controls whether the notebook plans, tests, or writes outputs\n",
    "* `optimize_output`: when `true`, runs `OPTIMIZE` on written silver Delta tables in `RUN` mode\n",
    "* `dq_threshold_pct`: maximum allowed null percentage for required columns in data quality gates\n",
    "\n",
    "Why these widgets remain:\n",
    "* They support production control, selective reruns, and troubleshooting\n",
    "* They do not control notebook dependencies; upstream bronze task completion is handled by the job DAG\n"
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
     "nuid": "4a39cf11-3dff-472c-accb-4c88d80e32f8",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Parameters and imports"
    }
   },
   "outputs": [],
   "source": [
    "# ==============================================================================\n",
    "# Notebook: Silver_Batch_Processing_Engine\n",
    "# Purpose   : Dynamic Bronze -> Silver batch processing framework for Canada Life\n",
    "#             Customer 360 with 21 silver outputs from 12 bronze tables.\n",
    "# Notes     :\n",
    "#             * Adobe bronze is line-broken JSON stored in a single string column.\n",
    "#               Only that source is JSON-flattened.\n",
    "#             * Other flattening steps are string/array explodes, not JSON flattening.\n",
    "#             * Default execution_mode is PLAN so the notebook is safe to run first.\n",
    "# ==============================================================================\n",
    "import re\n",
    "import uuid\n",
    "from functools import reduce\n",
    "\n",
    "from delta.tables import DeltaTable\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql import types as T\n",
    "from pyspark.sql.window import Window\n",
    "\n",
    "# ------------------------------------------------------------------------------\n",
    "# 1. Widget Parameters\n",
    "# ------------------------------------------------------------------------------\n",
    "for widget_name in [\n",
    "    \"target_table_name\",\n",
    "    \"run_date\",\n",
    "    \"execution_mode\",\n",
    "    \"optimize_output\",\n",
    "    \"dq_threshold_pct\",\n",
    "]:\n",
    "    try:\n",
    "        dbutils.widgets.remove(widget_name)\n",
    "    except Exception:\n",
    "        pass\n",
    "\n",
    "DEFAULT_CATALOG_NAME = \"dbw_c360_canadalife\"\n",
    "DEFAULT_BRONZE_SCHEMA = \"bronze\"\n",
    "DEFAULT_SILVER_SCHEMA = \"silver\"\n",
    "DEFAULT_SILVER_BASE_PATH = \"abfss://silver@adlsc360canadalife.dfs.core.windows.net\"\n",
    "\n",
    "widget_defaults = {\n",
    "    \"target_table_name\": \"ALL\",\n",
    "    \"run_date\": \"\",\n",
    "    \"execution_mode\": \"PLAN\",   # PLAN | TEST | RUN\n",
    "    \"optimize_output\": \"false\",\n",
    "    \"dq_threshold_pct\": \"2.0\",\n",
    "}\n",
    "\n",
    "for widget_name, default_value in widget_defaults.items():\n",
    "    dbutils.widgets.text(widget_name, default_value)\n",
    "\n",
    "catalog_name = DEFAULT_CATALOG_NAME\n",
    "bronze_schema = DEFAULT_BRONZE_SCHEMA\n",
    "silver_schema = DEFAULT_SILVER_SCHEMA\n",
    "target_table_name = dbutils.widgets.get(\"target_table_name\").strip() or \"ALL\"\n",
    "run_date = dbutils.widgets.get(\"run_date\").strip()\n",
    "silver_base_path = DEFAULT_SILVER_BASE_PATH\n",
    "execution_mode = (dbutils.widgets.get(\"execution_mode\").strip() or \"PLAN\").upper()\n",
    "optimize_output = dbutils.widgets.get(\"optimize_output\").strip().lower() == \"true\"\n",
    "dq_threshold_pct = float(dbutils.widgets.get(\"dq_threshold_pct\").strip() or \"2.0\") / 100.0\n",
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
     "nuid": "b5900032-ecf1-428c-a020-3ae1463fa772",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Configuration summary"
    }
   },
   "source": [
    "This section centralizes notebook metadata:\n",
    "* bronze source table mappings\n",
    "* expected source schemas for drift checks\n",
    "* target definitions and write modes\n",
    "* lookup dataframes and runtime caches\n"
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
     "nuid": "8a3e5269-f014-4f0e-ad21-5bbbd9280d1e",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Configuration and target metadata"
    }
   },
   "outputs": [],
   "source": [
    "# ------------------------------------------------------------------------------\n",
    "# 2. Constants and Configuration\n",
    "# ------------------------------------------------------------------------------\n",
    "BRONZE_TABLES = {\n",
    "    \"salesforce.crm\": \"salesforce_crm_bronze\",\n",
    "    \"ll_policy.individual_life\": \"ll_policy_individual_life_bronze\",\n",
    "    \"gwl_policy.individual_life\": \"gwl_policy_individual_life_bronze\",\n",
    "    \"sap_billing.invoices\": \"sap_billing_invoices_bronze\",\n",
    "    \"climl.seg_fund_contracts\": \"climl_seg_fund_contracts_bronze\",\n",
    "    \"call_centre.interactions\": \"call_centre_interactions_bronze\",\n",
    "    \"group_benefits.plan_members\": \"group_benefits_plan_members_bronze\",\n",
    "    \"freedom55.advisor_assignments\": \"freedom55_advisor_assignments_bronze\",\n",
    "    \"portal.digital_events\": \"portal_digital_events_bronze\",\n",
    "    \"reinsurance.treaty_data\": \"reinsurance_treaty_data_bronze\",\n",
    "    \"group_retirement.plan_members\": \"group_retirement_plan_members_bronze\",\n",
    "    \"adobe_analytics.digital_events\": \"adobe_analytics_digital_events_bronze\",\n",
    "}\n",
    "\n",
    "SOURCE_EXPECTED_COLUMNS = {\n",
    "    \"salesforce.crm\": {\"customer_id\", \"email\", \"phone\", \"province\", \"postal_code\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"ll_policy.individual_life\": {\"policy_id\", \"customer_id\", \"product_code\", \"face_amount\", \"premium\", \"frequency\", \"issue_date\", \"status\", \"province\", \"rider_codes\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"gwl_policy.individual_life\": {\"gwl_policy_id\", \"customer_id\", \"product\", \"sum_assured\", \"premium\", \"frequency\", \"issue_year\", \"status\", \"province\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"sap_billing.invoices\": {\"invoice_id\", \"policy_id\", \"customer_id\", \"billing_date\", \"amount\", \"status\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"climl.seg_fund_contracts\": {\"account_id\", \"customer_id\", \"asset_type\", \"fund_code\", \"market_value\", \"purchase_date\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"call_centre.interactions\": {\"interaction_id\", \"customer_id\", \"agent_id\", \"call_start_ts\", \"call_end_ts\", \"channel\", \"issue_type\", \"resolution_status\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"group_benefits.plan_members\": {\"member_id\", \"plan_id\", \"coverage_type\", \"province\", \"effective_date\", \"termination_date\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"freedom55.advisor_assignments\": {\"advisor_id\", \"customer_id\", \"assignment_id\", \"assignment_start_date\", \"assignment_end_date\", \"region\", \"channel\", \"status\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"portal.digital_events\": {\"claim_id\", \"policy_id\", \"customer_id\", \"claim_type\", \"claim_date\", \"status\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"reinsurance.treaty_data\": {\"treaty_id\", \"policy_id\", \"reinsurer\", \"ceded_amount\", \"status\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"group_retirement.plan_members\": {\"member_id\", \"plan_id\", \"employer_id\", \"retirement_date\", \"contribution_amount\", \"vesting_status\", \"member_status\", \"source_system\", \"processing_date\", \"ingestion_timestamp\"},\n",
    "    \"adobe_analytics.digital_events\": {\"event_id\", \"visitor_id\", \"session_id\", \"event_timestamp\", \"event_type\", \"page_name\", \"device_type\", \"campaign_id\", \"source_system\", \"processing_date\", \"ingestion_timestamp\", \"source_file_path\"},\n",
    "}\n",
    "\n",
    "SCD2_TRACKED_COLUMNS = [\n",
    "    \"policy_status_code\",\n",
    "    \"face_amount\",\n",
    "    \"premium_amount\",\n",
    "    \"premium_frequency_code\",\n",
    "    \"beneficiary_id\",\n",
    "    \"underwriting_class_code\",\n",
    "    \"rider_codes\",\n",
    "    \"occupational_class_code\",\n",
    "    \"dividend_option_code\",\n",
    "    \"smoker_status_code\",\n",
    "]\n",
    "\n",
    "TARGET_CONFIG = {\n",
    "    \"customer.master\": {\"kind\": \"business\", \"sources\": [\"salesforce.crm\", \"gwl_policy.individual_life\", \"sap_billing.invoices\", \"policy.individual_life_clean\"], \"keys\": [\"customer_id\"], \"write_mode\": \"overwrite\"},\n",
    "    \"policy.individual_life_clean\": {\"kind\": \"business\", \"sources\": [\"ll_policy.individual_life\", \"gwl_policy.individual_life\", \"salesforce.crm\"], \"keys\": [\"policy_number\"], \"write_mode\": \"scd2\"},\n",
    "    \"policy.individual_life_enriched\": {\"kind\": \"business\", \"sources\": [\"policy.individual_life_clean\"], \"keys\": [\"policy_number\", \"effective_date\"], \"write_mode\": \"overwrite\"},\n",
    "    \"policy.disability_ci_clean\": {\"kind\": \"business\", \"sources\": [\"ll_policy.individual_life\"], \"keys\": [\"policy_number\"], \"write_mode\": \"overwrite\"},\n",
    "    \"policy.policy_rider_detail\": {\"kind\": \"business\", \"sources\": [\"policy.individual_life_enriched\"], \"keys\": [\"policy_number\", \"rider_code\"], \"write_mode\": \"overwrite\"},\n",
    "    \"digital.portal_clean\": {\"kind\": \"business\", \"sources\": [\"adobe_analytics.digital_events\", \"portal.digital_events\"], \"keys\": [\"event_id\"], \"write_mode\": \"overwrite\"},\n",
    "    \"interactions.callcentre_clean\": {\"kind\": \"business\", \"sources\": [\"call_centre.interactions\"], \"keys\": [\"interaction_id\"], \"write_mode\": \"overwrite\"},\n",
    "    \"group_benefits.plan_clean\": {\"kind\": \"business\", \"sources\": [\"group_benefits.plan_members\"], \"keys\": [\"plan_id\", \"member_id\"], \"write_mode\": \"overwrite\"},\n",
    "    \"group_benefits.certificate_clean\": {\"kind\": \"business\", \"sources\": [\"group_benefits.plan_members\"], \"keys\": [\"certificate_number\"], \"write_mode\": \"overwrite\"},\n",
    "    \"group_benefits.certificate_coverage_detail\": {\"kind\": \"business\", \"sources\": [\"group_benefits.certificate_clean\"], \"keys\": [\"certificate_number\", \"coverage_type_code\"], \"write_mode\": \"overwrite\"},\n",
    "    \"freedom55.advisor_feed_clean\": {\"kind\": \"business\", \"sources\": [\"freedom55.advisor_assignments\"], \"keys\": [\"advisor_id\", \"assignment_id\"], \"write_mode\": \"overwrite\"},\n",
    "    \"investments.climl_clean\": {\"kind\": \"business\", \"sources\": [\"climl.seg_fund_contracts\"], \"keys\": [\"contract_number\", \"fund_code\"], \"write_mode\": \"overwrite\"},\n",
    "    \"investments.fund_allocation_detail\": {\"kind\": \"business\", \"sources\": [\"investments.climl_clean\"], \"keys\": [\"contract_number\", \"fund_code\"], \"write_mode\": \"overwrite\"},\n",
    "    \"group_retirement.member_clean\": {\"kind\": \"business\", \"sources\": [\"group_retirement.plan_members\"], \"keys\": [\"member_id\"], \"write_mode\": \"overwrite\"},\n",
    "    \"reinsurance.treaty_clean\": {\"kind\": \"business\", \"sources\": [\"reinsurance.treaty_data\"], \"keys\": [\"treaty_id\"], \"write_mode\": \"overwrite\"},\n",
    "    \"reference.product_code_mapping\": {\"kind\": \"reference\", \"sources\": [\"policy.individual_life_clean\", \"investments.climl_clean\", \"freedom55.advisor_feed_clean\"], \"keys\": [\"legacy_code\", \"source_system\"], \"write_mode\": \"overwrite\"},\n",
    "    \"reference.status_code_mapping\": {\"kind\": \"reference\", \"sources\": [\"policy.individual_life_clean\", \"reinsurance.treaty_clean\", \"interactions.callcentre_clean\"], \"keys\": [\"legacy_code\", \"source_system\"], \"write_mode\": \"overwrite\"},\n",
    "    \"reference.rider_codes\": {\"kind\": \"reference\", \"sources\": [\"policy.individual_life_clean\"], \"keys\": [\"rider_code\"], \"write_mode\": \"overwrite\"},\n",
    "    \"monitoring.schema_drift_log\": {\"kind\": \"monitoring\", \"sources\": list(BRONZE_TABLES.keys()), \"keys\": [\"source_name\", \"detected_at\", \"drift_type\"], \"write_mode\": \"append\"},\n",
    "    \"monitoring.dedup_audit_log\": {\"kind\": \"monitoring\", \"sources\": [\"policy.individual_life_clean\"], \"keys\": [\"policy_number\", \"_source_system\", \"_ingested_at\"], \"write_mode\": \"append\"},\n",
    "    \"monitoring.allocation_errors\": {\"kind\": \"monitoring\", \"sources\": [\"investments.fund_allocation_detail\"], \"keys\": [\"contract_number\", \"run_id\"], \"write_mode\": \"append\"},\n",
    "}\n",
    "\n",
    "TARGET_ORDER = [\n",
    "    \"customer.master\",\n",
    "    \"policy.individual_life_clean\",\n",
    "    \"policy.disability_ci_clean\",\n",
    "    \"digital.portal_clean\",\n",
    "    \"interactions.callcentre_clean\",\n",
    "    \"group_benefits.plan_clean\",\n",
    "    \"group_benefits.certificate_clean\",\n",
    "    \"freedom55.advisor_feed_clean\",\n",
    "    \"investments.climl_clean\",\n",
    "    \"group_retirement.member_clean\",\n",
    "    \"reinsurance.treaty_clean\",\n",
    "    \"reference.product_code_mapping\",\n",
    "    \"reference.status_code_mapping\",\n",
    "    \"reference.rider_codes\",\n",
    "    \"policy.individual_life_enriched\",\n",
    "    \"policy.policy_rider_detail\",\n",
    "    \"investments.fund_allocation_detail\",\n",
    "    \"group_benefits.certificate_coverage_detail\",\n",
    "    \"monitoring.schema_drift_log\",\n",
    "    \"monitoring.dedup_audit_log\",\n",
    "    \"monitoring.allocation_errors\",\n",
    "]\n",
    "\n",
    "PROVINCE_MAP_DATA = [\n",
    "    (\"Ontario\", \"ON\"), (\"British Columbia\", \"BC\"), (\"Alberta\", \"AB\"),\n",
    "    (\"Quebec\", \"QC\"), (\"Manitoba\", \"MB\"), (\"Saskatchewan\", \"SK\"),\n",
    "    (\"Nova Scotia\", \"NS\"), (\"New Brunswick\", \"NB\"),\n",
    "    (\"Newfoundland and Labrador\", \"NL\"), (\"Prince Edward Island\", \"PE\"),\n",
    "    (\"Northwest Territories\", \"NT\"), (\"Nunavut\", \"NU\"), (\"Yukon\", \"YT\"),\n",
    "]\n",
    "\n",
    "PROVINCE_MAP_DF = spark.createDataFrame(PROVINCE_MAP_DATA, [\"province_raw\", \"province_code\"])\n",
    "FREQ_MAP_DF = spark.createDataFrame(\n",
    "    [(\"M\", 12), (\"MONTHLY\", 12), (\"Q\", 4), (\"QUARTERLY\", 4), (\"S\", 2), (\"SEMI_ANNUAL\", 2), (\"A\", 1), (\"ANNUAL\", 1)],\n",
    "    [\"premium_frequency_code\", \"freq_multiplier\"],\n",
    ")\n",
    "\n",
    "DATAFRAME_CACHE = {}\n",
    "DEDUP_AUDIT_CACHE = None\n",
    "ALLOCATION_ERROR_CACHE = None\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a2b20473-b950-496e-a531-ed66609d4cdd",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Shared helpers"
    }
   },
   "source": [
    "These helper functions are reused across many targets.\n",
    "\n",
    "Highlights:\n",
    "* target and path name resolution\n",
    "* source filtering and schema inspection\n",
    "* PII cleaning and normalization\n",
    "* Delta write and SCD2 helpers\n",
    "* shared deduplication and union behavior\n"
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
     "nuid": "4e595532-b478-4d7c-af31-f16235736fc2",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Utility functions"
    }
   },
   "outputs": [],
   "source": [
    "# ------------------------------------------------------------------------------\n",
    "# 3. Utility Functions\n",
    "# ------------------------------------------------------------------------------\n",
    "def logical_to_physical(name: str) -> str:\n",
    "    return re.sub(r\"[^a-zA-Z0-9]+\", \"_\", name.strip()).strip(\"_\").lower()\n",
    "\n",
    "\n",
    "def bronze_table_fqn(source_name_value: str) -> str:\n",
    "    return f\"{catalog_name}.{bronze_schema}.{BRONZE_TABLES[source_name_value]}\"\n",
    "\n",
    "\n",
    "def silver_table_fqn(target_name_value: str) -> str:\n",
    "    return f\"{catalog_name}.{silver_schema}.{logical_to_physical(target_name_value)}\"\n",
    "\n",
    "\n",
    "def silver_storage_path(target_name_value: str) -> str:\n",
    "    return f\"{silver_base_path}/{catalog_name}/{silver_schema}/{target_name_value.replace('.', '/')}\"\n",
    "\n",
    "\n",
    "def table_exists(table_name: str) -> bool:\n",
    "    try:\n",
    "        return spark.catalog.tableExists(table_name)\n",
    "    except Exception:\n",
    "        return False\n",
    "\n",
    "\n",
    "def ensure_silver_schema() -> None:\n",
    "    spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}\")\n",
    "\n",
    "\n",
    "def normalise_filter_value(name: str) -> str:\n",
    "    return logical_to_physical(name)\n",
    "\n",
    "\n",
    "def resolve_selected_targets() -> list:\n",
    "    if target_table_name.upper() != \"ALL\":\n",
    "        selected = [target_name for target_name in TARGET_ORDER if normalise_filter_value(target_name) == normalise_filter_value(target_table_name)]\n",
    "        if not selected:\n",
    "            raise ValueError(f\"Target '{target_table_name}' is not configured.\")\n",
    "        return selected\n",
    "    return TARGET_ORDER\n",
    "\n",
    "\n",
    "def filter_incremental(df):\n",
    "    if not run_date:\n",
    "        return df\n",
    "    if \"processing_date\" in df.columns:\n",
    "        return df.filter(F.to_date(F.col(\"processing_date\")) == F.to_date(F.lit(run_date)))\n",
    "    if \"ingestion_timestamp\" in df.columns:\n",
    "        return df.filter(F.to_date(F.col(\"ingestion_timestamp\")) == F.to_date(F.lit(run_date)))\n",
    "    return df\n",
    "\n",
    "\n",
    "def get_column_names(df) -> set:\n",
    "    return set(df.columns)\n",
    "\n",
    "\n",
    "def safe_col(df_or_columns, column_name: str, dtype: str = \"string\"):\n",
    "    column_names = df_or_columns if isinstance(df_or_columns, set) else set(df_or_columns.columns)\n",
    "    return F.col(column_name) if column_name in column_names else F.lit(None).cast(dtype)\n",
    "\n",
    "\n",
    "def first_existing_column(df_or_columns, candidates: list, dtype: str = \"string\"):\n",
    "    column_names = df_or_columns if isinstance(df_or_columns, set) else set(df_or_columns.columns)\n",
    "    for candidate in candidates:\n",
    "        if candidate in column_names:\n",
    "            return F.col(candidate)\n",
    "    return F.lit(None).cast(dtype)\n",
    "\n",
    "\n",
    "def has_nested_field(schema: T.StructType, path: str) -> bool:\n",
    "    current_schema = schema\n",
    "    for index, part in enumerate(path.split(\".\")):\n",
    "        field_match = next((field for field in current_schema.fields if field.name == part), None)\n",
    "        if field_match is None:\n",
    "            return False\n",
    "        if index == len(path.split(\".\")) - 1:\n",
    "            return True\n",
    "        next_type = field_match.dataType\n",
    "        if isinstance(next_type, T.ArrayType):\n",
    "            next_type = next_type.elementType\n",
    "        if not isinstance(next_type, T.StructType):\n",
    "            return False\n",
    "        current_schema = next_type\n",
    "    return True\n",
    "\n",
    "\n",
    "def nested_or_null(df, candidates: list, dtype: str = \"string\"):\n",
    "    for candidate in candidates:\n",
    "        if \".\" in candidate:\n",
    "            if has_nested_field(df.schema, candidate):\n",
    "                return F.col(candidate).cast(dtype)\n",
    "        elif candidate in df.columns:\n",
    "            return F.col(candidate).cast(dtype)\n",
    "    return F.lit(None).cast(dtype)\n",
    "\n",
    "\n",
    "def build_batch_id_expr(df_or_columns):\n",
    "    column_names = df_or_columns if isinstance(df_or_columns, set) else set(df_or_columns.columns)\n",
    "    components = []\n",
    "    if \"source_file_path\" in column_names:\n",
    "        components.append(F.col(\"source_file_path\").cast(\"string\"))\n",
    "    if \"processing_date\" in column_names:\n",
    "        components.append(F.col(\"processing_date\").cast(\"string\"))\n",
    "    if \"ingestion_timestamp\" in column_names:\n",
    "        components.append(F.col(\"ingestion_timestamp\").cast(\"string\"))\n",
    "    if not components:\n",
    "        components = [F.lit(run_id)]\n",
    "    return F.sha2(F.concat_ws(\"||\", *components), 256)\n",
    "\n",
    "\n",
    "def apply_common_cleaning(df):\n",
    "    result_df = df\n",
    "    result_columns = get_column_names(result_df)\n",
    "    email_regex = r\"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$\"\n",
    "\n",
    "    email_source = F.lower(F.trim(first_existing_column(result_columns, [\"email\", \"email_address\"], \"string\")))\n",
    "    phone_source = F.trim(first_existing_column(result_columns, [\"phone\", \"phone_number\"], \"string\"))\n",
    "    postal_source = F.upper(F.trim(first_existing_column(result_columns, [\"postal_code\", \"postal_code_raw\"], \"string\")))\n",
    "    province_source = F.trim(first_existing_column(result_columns, [\"province\", \"province_name\"], \"string\"))\n",
    "    sin_source = F.trim(first_existing_column(result_columns, [\"sin\", \"social_insurance_number\"], \"string\"))\n",
    "\n",
    "    result_df = result_df.withColumn(\"email_raw\", F.when(email_source == \"\", F.lit(None).cast(\"string\")).otherwise(email_source))\n",
    "    result_df = result_df.withColumn(\n",
    "        \"email_clean\",\n",
    "        F.when(\n",
    "            F.col(\"email_raw\").rlike(email_regex),\n",
    "            F.concat(F.col(\"email_raw\").substr(1, 2), F.lit(\"***@\"), F.split(F.col(\"email_raw\"), \"@\").getItem(1)),\n",
    "        ).otherwise(F.lit(None).cast(\"string\")),\n",
    "    ).withColumn(\n",
    "        \"email_quality_flag\",\n",
    "        F.when(F.col(\"email_raw\").isNull(), F.lit(\"MISSING\")).when(~F.col(\"email_raw\").rlike(email_regex), F.lit(\"INVALID\")).otherwise(F.lit(\"VALID\")),\n",
    "    )\n",
    "\n",
    "    result_df = result_df.withColumn(\"phone_digits_raw\", F.regexp_replace(phone_source, r\"[^0-9]\", \"\"))\n",
    "    result_df = result_df.withColumn(\n",
    "        \"phone_digits\",\n",
    "        F.when(\n",
    "            (F.length(F.col(\"phone_digits_raw\")) == 11) & F.col(\"phone_digits_raw\").startswith(\"1\"),\n",
    "            F.col(\"phone_digits_raw\").substr(2, 10),\n",
    "        ).otherwise(F.col(\"phone_digits_raw\")),\n",
    "    )\n",
    "    result_df = result_df.withColumn(\n",
    "        \"phone_standardized\",\n",
    "        F.when(\n",
    "            (F.length(F.col(\"phone_digits\")) == 10) & F.col(\"phone_digits\").rlike(r\"^[2-9]\\d{2}[2-9]\\d{6}$\"),\n",
    "            F.concat(\n",
    "                F.col(\"phone_digits\").substr(1, 3),\n",
    "                F.lit(\"-\"),\n",
    "                F.col(\"phone_digits\").substr(4, 3),\n",
    "                F.lit(\"-\"),\n",
    "                F.col(\"phone_digits\").substr(7, 4),\n",
    "            ),\n",
    "        ).otherwise(F.lit(None).cast(\"string\")),\n",
    "    )\n",
    "    result_df = result_df.withColumn(\"phone_valid_flag\", F.col(\"phone_standardized\").isNotNull())\n",
    "    result_df = result_df.withColumn(\n",
    "        \"phone_clean\",\n",
    "        F.when(F.col(\"phone_standardized\").isNotNull(), F.concat(F.lit(\"***-***-\"), F.col(\"phone_digits\").substr(7, 4))).otherwise(F.lit(None).cast(\"string\")),\n",
    "    )\n",
    "\n",
    "    result_df = result_df.withColumn(\"province_raw_input\", F.when(province_source == \"\", F.lit(None).cast(\"string\")).otherwise(province_source))\n",
    "    result_df = result_df.join(\n",
    "        F.broadcast(PROVINCE_MAP_DF),\n",
    "        F.upper(F.trim(F.col(\"province_raw_input\"))) == F.upper(F.trim(F.col(\"province_raw\"))),\n",
    "        \"left\",\n",
    "    ).withColumn(\n",
    "        \"province_clean\",\n",
    "        F.coalesce(F.col(\"province_code\"), F.when(F.length(F.trim(F.col(\"province_raw_input\"))) == 2, F.upper(F.trim(F.col(\"province_raw_input\"))))),\n",
    "    ).drop(\"province_raw\", \"province_code\")\n",
    "\n",
    "    result_df = result_df.withColumn(\"postal_code_raw\", F.when(postal_source == \"\", F.lit(None).cast(\"string\")).otherwise(postal_source))\n",
    "    result_df = result_df.withColumn(\"postal_code_clean\", F.when(F.col(\"postal_code_raw\").isNotNull(), F.regexp_replace(F.col(\"postal_code_raw\"), r\"\\s+\", \"\")))\n",
    "    result_df = result_df.withColumn(\"postal_code_valid_flag\", F.col(\"postal_code_clean\").rlike(r\"^[A-Z]\\d[A-Z]\\d[A-Z]\\d$\"))\n",
    "    result_df = result_df.withColumn(\"sin_hashed\", F.when(sin_source.isNotNull(), F.sha2(sin_source.cast(\"string\"), 256)).otherwise(F.lit(None).cast(\"string\")))\n",
    "\n",
    "    for pii_column in [\"sin\", \"social_insurance_number\"]:\n",
    "        if pii_column in result_df.columns:\n",
    "            result_df = result_df.drop(pii_column)\n",
    "\n",
    "    # Drop intermediate scratch columns — only the clean/hashed outputs belong in silver\n",
    "    result_df = result_df.drop(\"email_raw\", \"phone_digits_raw\", \"phone_digits\", \"province_raw_input\", \"postal_code_raw\")\n",
    "\n",
    "    return result_df\n",
    "\n",
    "\n",
    "def enforce_null_gate(df, column_names: list, threshold: float):\n",
    "    valid_columns = [column_name for column_name in column_names if column_name in df.columns]\n",
    "    if not valid_columns:\n",
    "        return\n",
    "    total_rows = df.count()\n",
    "    if total_rows == 0:\n",
    "        return\n",
    "\n",
    "    agg_exprs = [F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias(column_name) for column_name in valid_columns]\n",
    "    null_counts = df.agg(*agg_exprs).collect()[0].asDict()\n",
    "    failures = []\n",
    "    for column_name, null_count in null_counts.items():\n",
    "        null_rate = (null_count or 0) / total_rows\n",
    "        if null_rate > threshold:\n",
    "            failures.append(f\"{column_name}={null_rate:.2%}\")\n",
    "    if failures:\n",
    "        raise ValueError(f\"[DQ GATE FAILED] Null threshold exceeded: {', '.join(failures)} > {threshold:.2%}\")\n",
    "\n",
    "\n",
    "IDENTITY_REVIEW_QUEUE_SCHEMA = \"compliance\"\n",
    "IDENTITY_REVIEW_QUEUE_TABLE_NAME = \"identity_resolution_manual_review\"\n",
    "IDENTITY_REVIEW_QUEUE_TABLE_FQN = f\"{catalog_name}.{IDENTITY_REVIEW_QUEUE_SCHEMA}.{IDENTITY_REVIEW_QUEUE_TABLE_NAME}\"\n",
    "IDENTITY_REVIEW_QUEUE_PATH = f\"{silver_base_path}/{catalog_name}/{IDENTITY_REVIEW_QUEUE_SCHEMA}/{IDENTITY_REVIEW_QUEUE_TABLE_NAME}\"\n",
    "IDENTITY_AUTO_MERGE_CACHE = None\n",
    "IDENTITY_REVIEW_QUEUE_CACHE = None\n",
    "\n",
    "\n",
    "def ensure_schema_exists(schema_name_value: str):\n",
    "    spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {schema_name_value}\")\n",
    "\n",
    "\n",
    "def _jaro_similarity(left_value, right_value):\n",
    "    left_string = (left_value or \"\").strip().upper()\n",
    "    right_string = (right_value or \"\").strip().upper()\n",
    "    if not left_string or not right_string:\n",
    "        return 0.0\n",
    "    if left_string == right_string:\n",
    "        return 1.0\n",
    "\n",
    "    left_len = len(left_string)\n",
    "    right_len = len(right_string)\n",
    "    match_distance = max(left_len, right_len) // 2 - 1\n",
    "    if match_distance < 0:\n",
    "        match_distance = 0\n",
    "\n",
    "    left_matches = [False] * left_len\n",
    "    right_matches = [False] * right_len\n",
    "    matches = 0\n",
    "\n",
    "    for left_index in range(left_len):\n",
    "        start_index = max(0, left_index - match_distance)\n",
    "        end_index = min(left_index + match_distance + 1, right_len)\n",
    "        for right_index in range(start_index, end_index):\n",
    "            if right_matches[right_index] or left_string[left_index] != right_string[right_index]:\n",
    "                continue\n",
    "            left_matches[left_index] = True\n",
    "            right_matches[right_index] = True\n",
    "            matches += 1\n",
    "            break\n",
    "\n",
    "    if matches == 0:\n",
    "        return 0.0\n",
    "\n",
    "    transpositions = 0\n",
    "    right_position = 0\n",
    "    for left_index in range(left_len):\n",
    "        if not left_matches[left_index]:\n",
    "            continue\n",
    "        while not right_matches[right_position]:\n",
    "            right_position += 1\n",
    "        if left_string[left_index] != right_string[right_position]:\n",
    "            transpositions += 1\n",
    "        right_position += 1\n",
    "\n",
    "    return ((matches / left_len) + (matches / right_len) + ((matches - transpositions / 2.0) / matches)) / 3.0\n",
    "\n",
    "\n",
    "def _jaro_winkler_similarity(left_value, right_value):\n",
    "    base_score = _jaro_similarity(left_value, right_value)\n",
    "    left_string = (left_value or \"\").strip().upper()\n",
    "    right_string = (right_value or \"\").strip().upper()\n",
    "    prefix_length = 0\n",
    "    for left_character, right_character in zip(left_string[:4], right_string[:4]):\n",
    "        if left_character != right_character:\n",
    "            break\n",
    "        prefix_length += 1\n",
    "    return float(base_score + (prefix_length * 0.1 * (1.0 - base_score)))\n",
    "\n",
    "\n",
    "JARO_WINKLER_UDF = F.udf(_jaro_winkler_similarity, T.DoubleType())\n",
    "\n",
    "\n",
    "def build_identity_resolution_artifacts(policy_df):\n",
    "    global IDENTITY_AUTO_MERGE_CACHE, IDENTITY_REVIEW_QUEUE_CACHE\n",
    "\n",
    "    if IDENTITY_AUTO_MERGE_CACHE is not None and IDENTITY_REVIEW_QUEUE_CACHE is not None:\n",
    "        return IDENTITY_AUTO_MERGE_CACHE, IDENTITY_REVIEW_QUEUE_CACHE\n",
    "\n",
    "    candidate_source_df = deduplicate_by_window(\n",
    "        policy_df.select(\n",
    "            \"customer_id\",\n",
    "            \"policy_number\",\n",
    "            \"date_of_birth\",\n",
    "            \"postal_code_clean\",\n",
    "            \"province_clean\",\n",
    "            \"email_clean\",\n",
    "            \"phone_clean\",\n",
    "            \"product_type_code\",\n",
    "            \"policy_status_code\",\n",
    "            \"beneficiary_id\",\n",
    "            \"_source_system\",\n",
    "            \"_ingested_at\",\n",
    "        ).filter(\n",
    "            F.col(\"customer_id\").isNotNull()\n",
    "            & F.col(\"date_of_birth\").isNotNull()\n",
    "            & F.col(\"postal_code_clean\").isNotNull()\n",
    "        ),\n",
    "        [\"customer_id\", \"_source_system\", \"postal_code_clean\", \"date_of_birth\"],\n",
    "        [\"_ingested_at\"],\n",
    "    )\n",
    "\n",
    "    candidate_rows = [row.asDict(recursive=True) for row in candidate_source_df.collect()]\n",
    "    blocked_groups = {}\n",
    "    for candidate_row in candidate_rows:\n",
    "        block_key = (candidate_row.get(\"postal_code_clean\"), candidate_row.get(\"date_of_birth\"))\n",
    "        blocked_groups.setdefault(block_key, []).append(candidate_row)\n",
    "\n",
    "    auto_merge_records = []\n",
    "    review_records = []\n",
    "    review_run_date = F.to_date(F.lit(run_date)) if run_date else None\n",
    "\n",
    "    for blocked_rows in blocked_groups.values():\n",
    "        if len(blocked_rows) < 2:\n",
    "            continue\n",
    "        for left_index in range(len(blocked_rows)):\n",
    "            for right_index in range(left_index + 1, len(blocked_rows)):\n",
    "                left_row = blocked_rows[left_index]\n",
    "                right_row = blocked_rows[right_index]\n",
    "                if left_row.get(\"customer_id\") == right_row.get(\"customer_id\"):\n",
    "                    continue\n",
    "                if left_row.get(\"_source_system\") == right_row.get(\"_source_system\"):\n",
    "                    continue\n",
    "\n",
    "                left_customer_id = left_row.get(\"customer_id\")\n",
    "                right_customer_id = right_row.get(\"customer_id\")\n",
    "                if left_customer_id > right_customer_id:\n",
    "                    left_row, right_row = right_row, left_row\n",
    "                    left_customer_id, right_customer_id = right_customer_id, left_customer_id\n",
    "\n",
    "                product_similarity_score = _jaro_winkler_similarity(left_row.get(\"product_type_code\"), right_row.get(\"product_type_code\"))\n",
    "                status_similarity_score = _jaro_winkler_similarity(left_row.get(\"policy_status_code\"), right_row.get(\"policy_status_code\"))\n",
    "                beneficiary_similarity_score = _jaro_winkler_similarity(left_row.get(\"beneficiary_id\"), right_row.get(\"beneficiary_id\"))\n",
    "                email_score = 0.18 if left_row.get(\"email_clean\") and right_row.get(\"email_clean\") and left_row.get(\"email_clean\") == right_row.get(\"email_clean\") else 0.0\n",
    "                phone_score = 0.12 if left_row.get(\"phone_clean\") and right_row.get(\"phone_clean\") and left_row.get(\"phone_clean\") == right_row.get(\"phone_clean\") else 0.0\n",
    "                province_score = 0.05 if left_row.get(\"province_clean\") and left_row.get(\"province_clean\") == right_row.get(\"province_clean\") else 0.0\n",
    "                match_confidence = round(\n",
    "                    0.55\n",
    "                    + email_score\n",
    "                    + phone_score\n",
    "                    + province_score\n",
    "                    + (product_similarity_score * 0.05)\n",
    "                    + (status_similarity_score * 0.03)\n",
    "                    + (beneficiary_similarity_score * 0.02),\n",
    "                    4,\n",
    "                )\n",
    "\n",
    "                if match_confidence >= 0.85:\n",
    "                    master_customer_id = min(left_customer_id, right_customer_id)\n",
    "                    auto_merge_records.append((left_customer_id, master_customer_id, match_confidence))\n",
    "                    auto_merge_records.append((right_customer_id, master_customer_id, match_confidence))\n",
    "                elif match_confidence >= 0.70:\n",
    "                    review_records.append((\n",
    "                        run_id,\n",
    "                        left_customer_id,\n",
    "                        right_customer_id,\n",
    "                        left_row.get(\"policy_number\"),\n",
    "                        right_row.get(\"policy_number\"),\n",
    "                        left_row.get(\"_source_system\"),\n",
    "                        right_row.get(\"_source_system\"),\n",
    "                        left_row.get(\"postal_code_clean\"),\n",
    "                        left_row.get(\"date_of_birth\"),\n",
    "                        match_confidence,\n",
    "                        round(product_similarity_score, 4),\n",
    "                        round(status_similarity_score, 4),\n",
    "                        round(beneficiary_similarity_score, 4),\n",
    "                        \"PENDING\",\n",
    "                        \"Blocked by postal_code_clean + date_of_birth with medium-confidence Jaro-Winkler composite score\",\n",
    "                    ))\n",
    "\n",
    "    auto_merge_schema = T.StructType([\n",
    "        T.StructField(\"customer_id\", T.StringType(), True),\n",
    "        T.StructField(\"master_customer_id\", T.StringType(), True),\n",
    "        T.StructField(\"identity_match_confidence\", T.DoubleType(), True),\n",
    "    ])\n",
    "    if auto_merge_records:\n",
    "        auto_merge_df = spark.createDataFrame(auto_merge_records, auto_merge_schema).groupBy(\"customer_id\").agg(\n",
    "            F.min(\"master_customer_id\").alias(\"master_customer_id\"),\n",
    "            F.max(\"identity_match_confidence\").alias(\"identity_match_confidence\"),\n",
    "        )\n",
    "    else:\n",
    "        auto_merge_df = spark.createDataFrame([], auto_merge_schema)\n",
    "\n",
    "    review_queue_schema = T.StructType([\n",
    "        T.StructField(\"run_id\", T.StringType(), True),\n",
    "        T.StructField(\"queued_at\", T.TimestampType(), True),\n",
    "        T.StructField(\"left_customer_id\", T.StringType(), True),\n",
    "        T.StructField(\"right_customer_id\", T.StringType(), True),\n",
    "        T.StructField(\"left_policy_number\", T.StringType(), True),\n",
    "        T.StructField(\"right_policy_number\", T.StringType(), True),\n",
    "        T.StructField(\"left_source_system\", T.StringType(), True),\n",
    "        T.StructField(\"right_source_system\", T.StringType(), True),\n",
    "        T.StructField(\"postal_code_clean\", T.StringType(), True),\n",
    "        T.StructField(\"date_of_birth\", T.DateType(), True),\n",
    "        T.StructField(\"match_confidence\", T.DoubleType(), True),\n",
    "        T.StructField(\"product_similarity_score\", T.DoubleType(), True),\n",
    "        T.StructField(\"status_similarity_score\", T.DoubleType(), True),\n",
    "        T.StructField(\"beneficiary_similarity_score\", T.DoubleType(), True),\n",
    "        T.StructField(\"review_status\", T.StringType(), True),\n",
    "        T.StructField(\"review_reason\", T.StringType(), True),\n",
    "    ])\n",
    "    if review_records:\n",
    "        review_queue_df = spark.createDataFrame(review_records, review_queue_schema).withColumn(\"queued_at\", F.current_timestamp())\n",
    "    else:\n",
    "        review_queue_df = spark.createDataFrame([], review_queue_schema).withColumn(\"queued_at\", F.current_timestamp())\n",
    "\n",
    "    IDENTITY_AUTO_MERGE_CACHE = auto_merge_df\n",
    "    IDENTITY_REVIEW_QUEUE_CACHE = review_queue_df\n",
    "    return IDENTITY_AUTO_MERGE_CACHE, IDENTITY_REVIEW_QUEUE_CACHE\n",
    "\n",
    "\n",
    "def write_identity_resolution_review_queue(review_queue_df):\n",
    "    ensure_schema_exists(f\"{catalog_name}.{IDENTITY_REVIEW_QUEUE_SCHEMA}\")\n",
    "    review_queue_df.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").save(IDENTITY_REVIEW_QUEUE_PATH)\n",
    "    spark.sql(\n",
    "        f\"CREATE TABLE IF NOT EXISTS {IDENTITY_REVIEW_QUEUE_TABLE_FQN} USING DELTA LOCATION '{IDENTITY_REVIEW_QUEUE_PATH}'\"\n",
    "    )\n",
    "\n",
    "\n",
    "def persist_identity_resolution_artifacts(policy_df):\n",
    "    auto_merge_df, review_queue_df = build_identity_resolution_artifacts(policy_df)\n",
    "    if execution_mode == \"RUN\":\n",
    "        write_identity_resolution_review_queue(review_queue_df)\n",
    "    return auto_merge_df, review_queue_df\n",
    "\n",
    "\n",
    "def ensure_external_table(target_name_value: str):\n",
    "    spark.sql(\n",
    "        f\"CREATE TABLE IF NOT EXISTS {silver_table_fqn(target_name_value)} USING DELTA LOCATION '{silver_storage_path(target_name_value)}'\"\n",
    "    )\n",
    "\n",
    "\n",
    "def write_delta(df, target_name_value: str, mode: str):\n",
    "    path = silver_storage_path(target_name_value)\n",
    "    projected_columns = []\n",
    "    seen_columns = set()\n",
    "    for column_name in df.columns:\n",
    "        if column_name not in seen_columns:\n",
    "            projected_columns.append(F.col(column_name).alias(column_name))\n",
    "            seen_columns.add(column_name)\n",
    "    output_df = df.select(*projected_columns)\n",
    "    if mode == \"append\":\n",
    "        output_df.write.format(\"delta\").mode(\"append\").option(\"mergeSchema\", \"true\").save(path)\n",
    "    else:\n",
    "        output_df.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").save(path)\n",
    "    ensure_external_table(target_name_value)\n",
    "\n",
    "\n",
    "def maybe_optimize(target_name_value: str, zorder_columns: list):\n",
    "    if not optimize_output or execution_mode != \"RUN\":\n",
    "        return\n",
    "    valid_columns = [column_name for column_name in zorder_columns if column_name]\n",
    "    table_name = silver_table_fqn(target_name_value)\n",
    "    if valid_columns:\n",
    "        spark.sql(f\"OPTIMIZE {table_name} ZORDER BY ({', '.join(valid_columns)})\")\n",
    "    else:\n",
    "        spark.sql(f\"OPTIMIZE {table_name}\")\n",
    "\n",
    "\n",
    "def apply_scd2(df, target_name_value: str, natural_key: str):\n",
    "    target_table = silver_table_fqn(target_name_value)\n",
    "    target_path = silver_storage_path(target_name_value)\n",
    "    today_expr = F.to_date(F.lit(run_date)) if run_date else F.current_date()\n",
    "\n",
    "    tracked_columns = [column_name for column_name in SCD2_TRACKED_COLUMNS if column_name in df.columns]\n",
    "    if not tracked_columns:\n",
    "        tracked_columns = [column_name for column_name in df.columns if column_name not in {natural_key, \"_ingested_at\", \"_batch_id\"}]\n",
    "\n",
    "    source_df = (\n",
    "        df.drop(\"effective_date\", \"expiry_date\", \"is_current\", \"_updated_at\")\n",
    "        .withColumn(\"effective_date\", today_expr)\n",
    "        .withColumn(\"expiry_date\", F.lit(None).cast(\"date\"))\n",
    "        .withColumn(\"is_current\", F.lit(True))\n",
    "        .withColumn(\"_updated_at\", F.current_timestamp())\n",
    "    )\n",
    "\n",
    "    if not table_exists(target_table):\n",
    "        source_df.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").save(target_path)\n",
    "        ensure_external_table(target_name_value)\n",
    "        return source_df\n",
    "\n",
    "    current_active_df = spark.table(target_table).filter(F.col(\"is_current\") == True)\n",
    "    comparison_df = source_df.alias(\"source\").join(\n",
    "        current_active_df.alias(\"target\"),\n",
    "        F.col(f\"source.{natural_key}\") == F.col(f\"target.{natural_key}\"),\n",
    "        \"left\",\n",
    "    )\n",
    "\n",
    "    change_condition = None\n",
    "    for column_name in tracked_columns:\n",
    "        if column_name in current_active_df.columns:\n",
    "            column_change = ~F.col(f\"source.{column_name}\").eqNullSafe(F.col(f\"target.{column_name}\"))\n",
    "            change_condition = column_change if change_condition is None else (change_condition | column_change)\n",
    "    if change_condition is None:\n",
    "        change_condition = F.lit(False)\n",
    "\n",
    "    changed_keys_df = comparison_df.filter(\n",
    "        F.col(f\"target.{natural_key}\").isNotNull() & change_condition\n",
    "    ).select(F.col(f\"source.{natural_key}\").alias(natural_key)).dropDuplicates([natural_key])\n",
    "\n",
    "    new_or_changed_df = comparison_df.filter(\n",
    "        F.col(f\"target.{natural_key}\").isNull() | change_condition\n",
    "    ).select(\"source.*\")\n",
    "\n",
    "    if changed_keys_df.limit(1).count() > 0:\n",
    "        expiry_updates_df = changed_keys_df.withColumn(\"new_expiry_date\", F.date_sub(today_expr, 1)).withColumn(\"updated_at\", F.current_timestamp())\n",
    "        DeltaTable.forPath(spark, target_path).alias(\"target\").merge(\n",
    "            expiry_updates_df.alias(\"updates\"),\n",
    "            f\"target.{natural_key} = updates.{natural_key} AND target.is_current = true\",\n",
    "        ).whenMatchedUpdate(set={\n",
    "            \"is_current\": \"false\",\n",
    "            \"expiry_date\": \"updates.new_expiry_date\",\n",
    "            \"_updated_at\": \"updates.updated_at\",\n",
    "        }).execute()\n",
    "\n",
    "    if new_or_changed_df.limit(1).count() > 0:\n",
    "        new_or_changed_df.write.format(\"delta\").mode(\"append\").option(\"mergeSchema\", \"true\").save(target_path)\n",
    "\n",
    "    ensure_external_table(target_name_value)\n",
    "    return spark.table(target_table)\n",
    "\n",
    "\n",
    "def deduplicate_by_window(df, keys: list, order_columns: list):\n",
    "    valid_keys = [column_name for column_name in keys if column_name in df.columns]\n",
    "    valid_order = [column_name for column_name in order_columns if column_name in df.columns]\n",
    "    if not valid_keys:\n",
    "        return df\n",
    "    if valid_order:\n",
    "        window_spec = Window.partitionBy(*valid_keys).orderBy(*[F.col(column_name).desc_nulls_last() for column_name in valid_order])\n",
    "        return df.withColumn(\"_row_num\", F.row_number().over(window_spec)).filter(F.col(\"_row_num\") == 1).drop(\"_row_num\")\n",
    "    return df.dropDuplicates(valid_keys)\n",
    "\n",
    "\n",
    "def union_all(dataframes: list):\n",
    "    if not dataframes:\n",
    "        raise ValueError(\"No dataframes to union\")\n",
    "    return reduce(lambda left_df, right_df: left_df.unionByName(right_df, allowMissingColumns=True), dataframes)\n",
    "\n",
    "\n",
    "def read_bronze_source(source_name_value: str):\n",
    "    df = spark.table(bronze_table_fqn(source_name_value))\n",
    "    return filter_incremental(df)\n",
    "\n",
    "\n",
    "def schema_signature(source_name_value: str):\n",
    "    source_df = spark.table(bronze_table_fqn(source_name_value))\n",
    "    columns = get_column_names(source_df)\n",
    "    expected = SOURCE_EXPECTED_COLUMNS[source_name_value]\n",
    "    unexpected = sorted(columns - expected)\n",
    "    missing = sorted(expected - columns)\n",
    "    return columns, unexpected, missing\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d09d9a3a-09dc-4626-b752-9d4a47fec4d9",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Source normalization"
    }
   },
   "source": [
    "Each helper in this section reads one bronze source and standardizes it for silver processing.\n",
    "\n",
    "Patterns used:\n",
    "* source-specific column mapping\n",
    "* safe column selection for evolving schemas\n",
    "* consistent `_ingested_at`, `_source_system`, and `_batch_id`\n",
    "* Adobe-only JSON flattening for digital events\n"
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
     "nuid": "66022584-22af-4ef2-91fe-d9694f6521ea",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Source normalization helpers"
    }
   },
   "outputs": [],
   "source": [
    "# ------------------------------------------------------------------------------\n",
    "# 4. Source Normalization Helpers\n",
    "# ------------------------------------------------------------------------------\n",
    "def normalize_salesforce_for_customer():\n",
    "    df = read_bronze_source(\"salesforce.crm\")\n",
    "    result_df = df.select(\n",
    "        safe_col(df, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(df, \"first_name\").alias(\"first_name\"),\n",
    "        safe_col(df, \"last_name\").alias(\"last_name\"),\n",
    "        safe_col(df, \"email\").alias(\"email\"),\n",
    "        safe_col(df, \"phone\").alias(\"phone\"),\n",
    "        safe_col(df, \"province\").alias(\"province\"),\n",
    "        safe_col(df, \"postal_code\").alias(\"postal_code\"),\n",
    "        safe_col(df, \"channel\").alias(\"channel\"),\n",
    "        safe_col(df, \"advisor_id\").alias(\"advisor_id\"),\n",
    "        safe_col(df, \"created_date\").cast(\"timestamp\").alias(\"created_date\"),\n",
    "        safe_col(df, \"updated_date\").cast(\"timestamp\").alias(\"updated_date\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        safe_col(df, \"source_system\").alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return apply_common_cleaning(result_df)\n",
    "\n",
    "\n",
    "def build_customer_contact_lookup():\n",
    "    salesforce_df = normalize_salesforce_for_customer().select(\n",
    "        \"customer_id\",\n",
    "        F.col(\"email_clean\").alias(\"contact_email_clean\"),\n",
    "        F.col(\"email_quality_flag\").alias(\"contact_email_quality_flag\"),\n",
    "        F.col(\"phone_clean\").alias(\"contact_phone_clean\"),\n",
    "        F.col(\"province_clean\").alias(\"contact_province_clean\"),\n",
    "        F.col(\"postal_code_clean\").alias(\"contact_postal_code_clean\"),\n",
    "        F.col(\"postal_code_valid_flag\").alias(\"contact_postal_code_valid_flag\"),\n",
    "    )\n",
    "    return deduplicate_by_window(salesforce_df, [\"customer_id\"], [])\n",
    "\n",
    "\n",
    "def normalize_gwl_policy():\n",
    "    df = read_bronze_source(\"gwl_policy.individual_life\")\n",
    "    gwl_policy_id = F.coalesce(\n",
    "        safe_col(df, \"gwl_policy_id\"),\n",
    "        safe_col(df, \"policy_id\"),\n",
    "        F.sha2(F.concat_ws(\"||\", *[F.col(column_name).cast(\"string\") for column_name in df.columns]), 256),\n",
    "    )\n",
    "    result_df = df.select(\n",
    "        gwl_policy_id.alias(\"policy_number\"),\n",
    "        gwl_policy_id.alias(\"legacy_policy_number\"),\n",
    "        safe_col(df, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(df, \"product\").alias(\"product_type_code\"),\n",
    "        safe_col(df, \"sum_assured\").cast(T.DecimalType(15, 2)).alias(\"face_amount\"),\n",
    "        safe_col(df, \"premium\").cast(T.DecimalType(12, 2)).alias(\"premium_amount\"),\n",
    "        F.upper(safe_col(df, \"frequency\")).alias(\"premium_frequency_code\"),\n",
    "        F.to_date(F.concat_ws(\"-\", safe_col(df, \"issue_year\"), F.lit(\"01\"), F.lit(\"01\")), \"yyyy-MM-dd\").alias(\"issue_date\"),\n",
    "        safe_col(df, \"expiry_date\").cast(\"date\").alias(\"expiry_date\"),\n",
    "        safe_col(df, \"status\").alias(\"policy_status_code\"),\n",
    "        F.lit(None).cast(\"string\").alias(\"beneficiary_id\"),\n",
    "        F.lit(None).cast(\"string\").alias(\"rider_codes\"),\n",
    "        F.lit(None).cast(\"string\").alias(\"underwriting_class_code\"),\n",
    "        safe_col(df, \"province\").alias(\"province\"),\n",
    "        safe_col(df, \"postal_code\").alias(\"postal_code\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        F.coalesce(safe_col(df, \"source_system\"), F.lit(\"gwl_policy_admin\")).alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return apply_common_cleaning(result_df)\n",
    "\n",
    "\n",
    "def normalize_ll_policy():\n",
    "    df = read_bronze_source(\"ll_policy.individual_life\")\n",
    "    ll_policy_id = F.coalesce(\n",
    "        safe_col(df, \"policy_id\"),\n",
    "        F.sha2(F.concat_ws(\"||\", *[F.col(column_name).cast(\"string\") for column_name in df.columns]), 256),\n",
    "    )\n",
    "    result_df = df.select(\n",
    "        F.concat(F.lit(\"LL-\"), ll_policy_id).alias(\"policy_number\"),\n",
    "        ll_policy_id.alias(\"legacy_policy_number\"),\n",
    "        safe_col(df, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(df, \"product_code\").alias(\"product_type_code\"),\n",
    "        safe_col(df, \"face_amount\").cast(T.DecimalType(15, 2)).alias(\"face_amount\"),\n",
    "        safe_col(df, \"premium\").cast(T.DecimalType(12, 2)).alias(\"premium_amount\"),\n",
    "        F.upper(safe_col(df, \"frequency\")).alias(\"premium_frequency_code\"),\n",
    "        safe_col(df, \"issue_date\").cast(\"date\").alias(\"issue_date\"),\n",
    "        safe_col(df, \"expiry_date\").cast(\"date\").alias(\"expiry_date\"),\n",
    "        safe_col(df, \"status\").alias(\"policy_status_code\"),\n",
    "        safe_col(df, \"beneficiary\").alias(\"beneficiary_id\"),\n",
    "        safe_col(df, \"rider_codes\").alias(\"rider_codes\"),\n",
    "        safe_col(df, \"underwriter\").alias(\"underwriting_class_code\"),\n",
    "        safe_col(df, \"province\").alias(\"province\"),\n",
    "        safe_col(df, \"postal_code\").alias(\"postal_code\"),\n",
    "        safe_col(df, \"sin\").alias(\"sin\"),\n",
    "        safe_col(df, \"dob\").cast(\"date\").alias(\"date_of_birth\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        F.coalesce(safe_col(df, \"source_system\"), F.lit(\"ll_policy_admin\")).alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return apply_common_cleaning(result_df)\n",
    "\n",
    "\n",
    "def normalize_portal_events():\n",
    "    df = read_bronze_source(\"portal.digital_events\")\n",
    "    result_df = df.select(\n",
    "        F.sha2(F.concat_ws(\"||\", safe_col(df, \"claim_id\"), safe_col(df, \"policy_id\"), safe_col(df, \"customer_id\"), safe_col(df, \"processing_date\")), 256).alias(\"event_id\"),\n",
    "        safe_col(df, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(df, \"policy_id\").alias(\"policy_number\"),\n",
    "        safe_col(df, \"claim_type\").alias(\"event_type\"),\n",
    "        F.coalesce(safe_col(df, \"reported_date\").cast(\"timestamp\"), safe_col(df, \"claim_date\").cast(\"timestamp\"), safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\")).alias(\"event_timestamp\"),\n",
    "        safe_col(df, \"status\").alias(\"event_status\"),\n",
    "        safe_col(df, \"notes\").alias(\"event_notes\"),\n",
    "        safe_col(df, \"source_system\").alias(\"_source_system\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return result_df\n",
    "\n",
    "\n",
    "def normalize_callcentre_interactions():\n",
    "    df = read_bronze_source(\"call_centre.interactions\")\n",
    "    result_df = df.select(\n",
    "        safe_col(df, \"interaction_id\").alias(\"interaction_id\"),\n",
    "        safe_col(df, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(df, \"agent_id\").alias(\"agent_id\"),\n",
    "        safe_col(df, \"call_start_ts\").cast(\"timestamp\").alias(\"call_start_ts\"),\n",
    "        safe_col(df, \"call_end_ts\").cast(\"timestamp\").alias(\"call_end_ts\"),\n",
    "        safe_col(df, \"channel\").alias(\"channel\"),\n",
    "        safe_col(df, \"issue_type\").alias(\"issue_type\"),\n",
    "        safe_col(df, \"resolution_status\").alias(\"interaction_status\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        F.coalesce(safe_col(df, \"source_system\"), F.lit(\"avaya_call_centre\")).alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    result_df = result_df.withColumn(\n",
    "        \"call_duration_minutes\",\n",
    "        F.when(\n",
    "            F.col(\"call_end_ts\").isNotNull() & F.col(\"call_start_ts\").isNotNull(),\n",
    "            (F.col(\"call_end_ts\").cast(\"long\") - F.col(\"call_start_ts\").cast(\"long\")) / 60.0,\n",
    "        ).cast(\"double\"),\n",
    "    )\n",
    "    return result_df\n",
    "\n",
    "\n",
    "def normalize_group_benefits_base():\n",
    "    df = read_bronze_source(\"group_benefits.plan_members\")\n",
    "    coverage_fragments = []\n",
    "    if \"coverage_type\" in df.columns:\n",
    "        coverage_fragments.append(F.col(\"coverage_type\"))\n",
    "    for column_name, value_name in [(\"dental\", \"GRP_DENTAL\"), (\"vision\", \"GRP_VISION\"), (\"ltd\", \"GRP_LTD\")]:\n",
    "        if column_name in df.columns:\n",
    "            coverage_fragments.append(F.when(F.upper(F.col(column_name).cast(\"string\")).isin(\"Y\", \"YES\", \"TRUE\", \"1\"), F.lit(value_name)))\n",
    "    coverage_array = F.array(*coverage_fragments) if coverage_fragments else F.array(F.lit(None))\n",
    "\n",
    "    result_df = df.select(\n",
    "        safe_col(df, \"member_id\").alias(\"member_id\"),\n",
    "        safe_col(df, \"plan_id\").alias(\"plan_id\"),\n",
    "        F.concat(F.lit(\"CERT-\"), safe_col(df, \"member_id\")).alias(\"certificate_number\"),\n",
    "        safe_col(df, \"employer\").alias(\"employer_name\"),\n",
    "        safe_col(df, \"first_name\").alias(\"first_name\"),\n",
    "        safe_col(df, \"last_name\").alias(\"last_name\"),\n",
    "        safe_col(df, \"dob\").cast(\"date\").alias(\"date_of_birth\"),\n",
    "        safe_col(df, \"province\").alias(\"province\"),\n",
    "        safe_col(df, \"effective_date\").cast(\"date\").alias(\"effective_date\"),\n",
    "        safe_col(df, \"termination_date\").cast(\"date\").alias(\"termination_date\"),\n",
    "        F.array_join(F.array_distinct(F.filter(coverage_array, lambda x: x.isNotNull())), \",\").alias(\"coverage_type_codes_enrolled\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        safe_col(df, \"source_system\").alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return apply_common_cleaning(result_df)\n",
    "\n",
    "\n",
    "def normalize_freedom55_assignments():\n",
    "    df = read_bronze_source(\"freedom55.advisor_assignments\")\n",
    "    result_df = df.select(\n",
    "        safe_col(df, \"assignment_id\").alias(\"assignment_id\"),\n",
    "        safe_col(df, \"advisor_id\").alias(\"advisor_id\"),\n",
    "        safe_col(df, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(df, \"assignment_start_date\").cast(\"date\").alias(\"assignment_start_date\"),\n",
    "        safe_col(df, \"assignment_end_date\").cast(\"date\").alias(\"assignment_end_date\"),\n",
    "        safe_col(df, \"region\").alias(\"region\"),\n",
    "        safe_col(df, \"channel\").alias(\"channel\"),\n",
    "        safe_col(df, \"status\").alias(\"advisor_assignment_status\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        F.coalesce(safe_col(df, \"source_system\"), F.lit(\"f55_advisor\")).alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "        F.lit(None).cast(\"string\").alias(\"policy_number\"),\n",
    "        F.lit(None).cast(\"string\").alias(\"product_type_code\"),\n",
    "    )\n",
    "    return result_df\n",
    "\n",
    "\n",
    "def normalize_climl_contracts():\n",
    "    df = read_bronze_source(\"climl.seg_fund_contracts\")\n",
    "    result_df = df.select(\n",
    "        safe_col(df, \"account_id\").alias(\"contract_number\"),\n",
    "        safe_col(df, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(df, \"asset_type\").alias(\"product_type_code\"),\n",
    "        safe_col(df, \"fund_code\").alias(\"fund_code\"),\n",
    "        safe_col(df, \"units\").cast(\"double\").alias(\"units\"),\n",
    "        safe_col(df, \"nav\").cast(\"double\").alias(\"nav\"),\n",
    "        safe_col(df, \"market_value\").cast(\"double\").alias(\"market_value\"),\n",
    "        safe_col(df, \"purchase_date\").cast(\"date\").alias(\"purchase_date\"),\n",
    "        safe_col(df, \"currency\").alias(\"currency\"),\n",
    "        safe_col(df, \"benchmark\").alias(\"benchmark\"),\n",
    "        safe_col(df, \"ytd_return_pct\").cast(\"double\").alias(\"ytd_return_pct\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        safe_col(df, \"source_system\").alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return result_df\n",
    "\n",
    "\n",
    "def normalize_group_retirement_members():\n",
    "    df = read_bronze_source(\"group_retirement.plan_members\")\n",
    "    result_df = df.select(\n",
    "        safe_col(df, \"member_id\").alias(\"member_id\"),\n",
    "        safe_col(df, \"plan_id\").alias(\"plan_id\"),\n",
    "        safe_col(df, \"employer_id\").alias(\"employer_id\"),\n",
    "        safe_col(df, \"retirement_date\").cast(\"date\").alias(\"retirement_date\"),\n",
    "        safe_col(df, \"contribution_amount\").cast(T.DecimalType(15, 2)).alias(\"contribution_amount\"),\n",
    "        safe_col(df, \"vesting_status\").alias(\"vesting_status\"),\n",
    "        safe_col(df, \"member_status\").alias(\"member_status\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        F.coalesce(safe_col(df, \"source_system\"), F.lit(\"group_retirement\")).alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return result_df\n",
    "\n",
    "\n",
    "def normalize_reinsurance_treaties():\n",
    "    df = read_bronze_source(\"reinsurance.treaty_data\")\n",
    "    result_df = df.select(\n",
    "        safe_col(df, \"treaty_id\").alias(\"treaty_id\"),\n",
    "        safe_col(df, \"policy_id\").alias(\"policy_number\"),\n",
    "        safe_col(df, \"reinsurer\").alias(\"reinsurer_name\"),\n",
    "        safe_col(df, \"ceded_amount\").cast(T.DecimalType(15, 2)).alias(\"ceded_amount\"),\n",
    "        safe_col(df, \"retained_amount\").cast(T.DecimalType(15, 2)).alias(\"retained_amount\"),\n",
    "        safe_col(df, \"premium_ceded\").cast(T.DecimalType(15, 2)).alias(\"premium_ceded\"),\n",
    "        safe_col(df, \"claim_recovered\").cast(T.DecimalType(15, 2)).alias(\"claim_recovered\"),\n",
    "        safe_col(df, \"effective_date\").cast(\"date\").alias(\"effective_date\"),\n",
    "        safe_col(df, \"expiry_date\").cast(\"date\").alias(\"expiry_date\"),\n",
    "        safe_col(df, \"product\").alias(\"product_type_code\"),\n",
    "        safe_col(df, \"status\").alias(\"policy_status_code\"),\n",
    "        safe_col(df, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        safe_col(df, \"source_system\").alias(\"_source_system\"),\n",
    "        build_batch_id_expr(df).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return result_df\n",
    "\n",
    "\n",
    "def read_adobe_json_events():\n",
    "    bronze_df = read_bronze_source(\"adobe_analytics.digital_events\")\n",
    "    path_rows = bronze_df.select(\"source_file_path\").where(F.col(\"source_file_path\").isNotNull()).distinct().collect()\n",
    "    json_paths = [row[0] for row in path_rows if row[0]]\n",
    "    if not json_paths:\n",
    "        schema = T.StructType([\n",
    "            T.StructField(\"event_id\", T.StringType(), True),\n",
    "            T.StructField(\"customer_id\", T.StringType(), True),\n",
    "            T.StructField(\"policy_number\", T.StringType(), True),\n",
    "            T.StructField(\"event_type\", T.StringType(), True),\n",
    "            T.StructField(\"event_timestamp\", T.TimestampType(), True),\n",
    "            T.StructField(\"journey_name\", T.StringType(), True),\n",
    "            T.StructField(\"page_name\", T.StringType(), True),\n",
    "            T.StructField(\"raw_event_json\", T.StringType(), True),\n",
    "            T.StructField(\"_source_system\", T.StringType(), True),\n",
    "            T.StructField(\"_ingested_at\", T.TimestampType(), True),\n",
    "            T.StructField(\"_batch_id\", T.StringType(), True),\n",
    "        ])\n",
    "        return spark.createDataFrame([], schema)\n",
    "\n",
    "    raw_json_df = spark.read.option(\"multiLine\", True).json(json_paths)\n",
    "    flattened_df = raw_json_df\n",
    "    for candidate_array in [\"events\", \"data\", \"rows\", \"items\", \"records\"]:\n",
    "        flattened_columns = get_column_names(flattened_df)\n",
    "        if candidate_array in flattened_columns:\n",
    "            flattened_df = flattened_df.withColumn(candidate_array, F.explode_outer(F.col(candidate_array)))\n",
    "            candidate_dtype = flattened_df.schema[candidate_array].dataType\n",
    "            if isinstance(candidate_dtype, T.StructType):\n",
    "                inner_field_names = candidate_dtype.names\n",
    "                inner_fields = [F.col(f\"{candidate_array}.{field_name}\").alias(field_name) for field_name in inner_field_names]\n",
    "                passthrough_fields = [F.col(column_name) for column_name in flattened_df.columns if column_name != candidate_array]\n",
    "                flattened_df = flattened_df.select(*passthrough_fields, *inner_fields)\n",
    "            break\n",
    "\n",
    "    raw_event_struct = F.struct(*[F.col(column_name) for column_name in flattened_df.columns])\n",
    "\n",
    "    result_df = flattened_df.select(\n",
    "        F.coalesce(\n",
    "            nested_or_null(flattened_df, [\"event_id\", \"eventId\", \"id\", \"hitid_high\", \"visit_num\"], \"string\"),\n",
    "            F.sha2(F.to_json(raw_event_struct), 256),\n",
    "        ).alias(\"event_id\"),\n",
    "        nested_or_null(flattened_df, [\"customer_id\", \"customerId\", \"identity.customer_id\", \"identity.customerId\", \"cust_id\"], \"string\").alias(\"customer_id\"),\n",
    "        nested_or_null(flattened_df, [\"policy_number\", \"policy_id\", \"policyId\", \"policy.id\"], \"string\").alias(\"policy_number\"),\n",
    "        nested_or_null(flattened_df, [\"event_type\", \"eventType\", \"type\", \"page_event\"], \"string\").alias(\"event_type\"),\n",
    "        F.coalesce(\n",
    "            nested_or_null(flattened_df, [\"event_timestamp\", \"timestamp\", \"ts\", \"occurred_at\", \"api_metadata.extracted_at\"], \"timestamp\"),\n",
    "            F.current_timestamp(),\n",
    "        ).alias(\"event_timestamp\"),\n",
    "        nested_or_null(flattened_df, [\"journey_name\", \"journeyName\", \"journey.name\"], \"string\").alias(\"journey_name\"),\n",
    "        nested_or_null(flattened_df, [\"page_name\", \"pageName\", \"page.name\", \"page_url\"], \"string\").alias(\"page_name\"),\n",
    "        F.to_json(raw_event_struct).alias(\"raw_event_json\"),\n",
    "        F.lit(\"adobe_analytics.digital_events\").alias(\"_source_system\"),\n",
    "        F.current_timestamp().alias(\"_ingested_at\"),\n",
    "        F.lit(run_id).alias(\"_batch_id\"),\n",
    "    )\n",
    "    return result_df\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "20503c4a-987f-4ad6-a868-e6b62b42b2e0",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Target builders and orchestration"
    }
   },
   "source": [
    "This section assembles normalized sources into the final silver outputs.\n",
    "\n",
    "It includes:\n",
    "* business target builders\n",
    "* reference and monitoring builders\n",
    "* a target dispatch map\n",
    "* the main execution loop for PLAN, TEST, and RUN\n"
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
     "nuid": "bfdc225e-e195-47db-9474-aa796e73bd10",
     "showTitle": true,
     "tableResultSettingsMap": {
      "0": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{},\"columnVisibility\":{}},\"settings\":{\"columns\":{}},\"syncTimestamp\":1780731849310}",
       "filterBlob": null,
       "queryPlanFiltersBlob": null,
       "tableResultIndex": 0
      },
      "1": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{},\"columnVisibility\":{}},\"settings\":{\"columns\":{}},\"syncTimestamp\":1780732340111}",
       "filterBlob": null,
       "queryPlanFiltersBlob": null,
       "tableResultIndex": 1
      }
     },
     "title": "Target builders and execution"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>first_name</th><th>last_name</th><th>email_clean</th><th>email_quality_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>channel</th><th>advisor_id</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>product_type_code</th><th>policy_number</th><th>billing_status</th><th>billing_amount</th><th>source_customer_id</th></tr></thead><tbody><tr><td>0001d66f-b024-4226-8d59-dc7ea6d53792</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2026-06-08T17:45:17.234Z</td><td>sap_billing</td><td>cb783f695b74071849fda6e4f18e56ff3767a2ac487f0aa76ba43c7d8acb306d</td><td>null</td><td>POL-72073314</td><td>Paid</td><td>5365.62</td><td>0001d66f-b024-4226-8d59-dc7ea6d53792</td></tr><tr><td>0002c48c-4076-4d07-af81-a1db70fb96bf</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>ON</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2026-06-08T17:44:36.101Z</td><td>gwl_policy_admin</td><td>7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2</td><td>TFSA</td><td>null</td><td>null</td><td>null</td><td>0002c48c-4076-4d07-af81-a1db70fb96bf</td></tr><tr><td>00034ee3-6d84-4222-8402-a0b0e56eee19</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>QC</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2026-06-08T17:44:36.101Z</td><td>gwl_policy_admin</td><td>7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2</td><td>Term Life</td><td>null</td><td>null</td><td>null</td><td>00034ee3-6d84-4222-8402-a0b0e56eee19</td></tr><tr><td>00088100-c961-4f45-8ad0-c8d9871b5d14</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2026-06-08T17:45:17.234Z</td><td>sap_billing</td><td>cb783f695b74071849fda6e4f18e56ff3767a2ac487f0aa76ba43c7d8acb306d</td><td>null</td><td>POL-59267941</td><td>Waived</td><td>2702.09</td><td>00088100-c961-4f45-8ad0-c8d9871b5d14</td></tr><tr><td>000afdd1-f46b-438f-9bc0-3d950c24718b</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2026-06-08T17:45:17.234Z</td><td>sap_billing</td><td>cb783f695b74071849fda6e4f18e56ff3767a2ac487f0aa76ba43c7d8acb306d</td><td>null</td><td>POL-67664228</td><td>Waived</td><td>6789.61</td><td>000afdd1-f46b-438f-9bc0-3d950c24718b</td></tr></tbody></table></div>"
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
         "0001d66f-b024-4226-8d59-dc7ea6d53792",
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         "2026-06-08T17:45:17.234Z",
         "sap_billing",
         "cb783f695b74071849fda6e4f18e56ff3767a2ac487f0aa76ba43c7d8acb306d",
         null,
         "POL-72073314",
         "Paid",
         "5365.62",
         "0001d66f-b024-4226-8d59-dc7ea6d53792"
        ],
        [
         "0002c48c-4076-4d07-af81-a1db70fb96bf",
         null,
         null,
         null,
         null,
         null,
         "ON",
         null,
         null,
         null,
         null,
         "2026-06-08T17:44:36.101Z",
         "gwl_policy_admin",
         "7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2",
         "TFSA",
         null,
         null,
         null,
         "0002c48c-4076-4d07-af81-a1db70fb96bf"
        ],
        [
         "00034ee3-6d84-4222-8402-a0b0e56eee19",
         null,
         null,
         null,
         null,
         null,
         "QC",
         null,
         null,
         null,
         null,
         "2026-06-08T17:44:36.101Z",
         "gwl_policy_admin",
         "7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2",
         "Term Life",
         null,
         null,
         null,
         "00034ee3-6d84-4222-8402-a0b0e56eee19"
        ],
        [
         "00088100-c961-4f45-8ad0-c8d9871b5d14",
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         "2026-06-08T17:45:17.234Z",
         "sap_billing",
         "cb783f695b74071849fda6e4f18e56ff3767a2ac487f0aa76ba43c7d8acb306d",
         null,
         "POL-59267941",
         "Waived",
         "2702.09",
         "00088100-c961-4f45-8ad0-c8d9871b5d14"
        ],
        [
         "000afdd1-f46b-438f-9bc0-3d950c24718b",
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         "2026-06-08T17:45:17.234Z",
         "sap_billing",
         "cb783f695b74071849fda6e4f18e56ff3767a2ac487f0aa76ba43c7d8acb306d",
         null,
         "POL-67664228",
         "Waived",
         "6789.61",
         "000afdd1-f46b-438f-9bc0-3d950c24718b"
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
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "first_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "last_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "channel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "advisor_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "billing_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "billing_amount",
         "type": "\"decimal(12,2)\""
        },
        {
         "metadata": "{}",
         "name": "source_customer_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>policy_number</th><th>legacy_policy_number</th><th>product_type_code</th><th>face_amount</th><th>premium_amount</th><th>premium_frequency_code</th><th>issue_date</th><th>expiry_date</th><th>policy_status_code</th><th>beneficiary_id</th><th>rider_codes</th><th>underwriting_class_code</th><th>province</th><th>postal_code</th><th>date_of_birth</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>email_clean</th><th>email_quality_flag</th><th>phone_standardized</th><th>phone_valid_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>sin_hashed</th><th>_ingested_year</th><th>_ingested_month</th><th>master_customer_id</th><th>identity_match_confidence</th><th>identity_manual_review_flag</th><th>identity_resolution_status</th></tr></thead><tbody><tr><td>00030cef-1862-4d2b-8e5e-a5c4f8a6cb1c</td><td>LL-POL-26474533</td><td>POL-26474533</td><td>Term Life</td><td>1791723.30</td><td>4513.05</td><td>MONTHLY</td><td>2011-02-17</td><td>2031-01-10</td><td>Suspended</td><td>Katherine Cooke</td><td>WAIVER</td><td>Cindy Cooper</td><td>ON</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>ON</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>00030cef-1862-4d2b-8e5e-a5c4f8a6cb1c</td><td>null</td><td>false</td><td>DISTINCT</td></tr><tr><td>0007accc-c5c0-429a-83ea-a673045af2b4</td><td>LL-POL-93981696</td><td>POL-93981696</td><td>Whole Life</td><td>1115529.08</td><td>2902.40</td><td>ANNUAL</td><td>2013-06-21</td><td>2029-02-18</td><td>Pending</td><td>Jonathan King</td><td>WAIVER</td><td>Julie Scott</td><td>PE</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>0007accc-c5c0-429a-83ea-a673045af2b4</td><td>null</td><td>false</td><td>DISTINCT</td></tr><tr><td>0017489d-083b-42c9-96e6-a5871317c79f</td><td>GWL-51597224</td><td>GWL-51597224</td><td>Group Benefits</td><td>9011070.60</td><td>7287.37</td><td>MONTHLY</td><td>1996-01-01</td><td>null</td><td>Lapsed</td><td>null</td><td>null</td><td>null</td><td>MB</td><td>null</td><td>null</td><td>2026-06-08T17:44:36.101Z</td><td>gwl_policy_admin</td><td>7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>MB</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>0017489d-083b-42c9-96e6-a5871317c79f</td><td>null</td><td>false</td><td>DISTINCT</td></tr><tr><td>001b8378-e54a-435e-8307-5bdd98f48ff4</td><td>GWL-55103744</td><td>GWL-55103744</td><td>GIA</td><td>4530779.44</td><td>118.38</td><td>ANNUAL</td><td>2006-01-01</td><td>null</td><td>Active</td><td>null</td><td>null</td><td>null</td><td>BC</td><td>null</td><td>null</td><td>2026-06-08T17:44:36.101Z</td><td>gwl_policy_admin</td><td>7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>BC</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>001b8378-e54a-435e-8307-5bdd98f48ff4</td><td>null</td><td>false</td><td>DISTINCT</td></tr><tr><td>001eeb60-456e-4817-a52a-6e92d94e25f0</td><td>LL-POL-77675052</td><td>POL-77675052</td><td>Term Life</td><td>1491499.38</td><td>1254.64</td><td>ANNUAL</td><td>2015-12-23</td><td>2032-05-09</td><td>Pending</td><td>Robert Ross MD</td><td>null</td><td>Christopher Knapp</td><td>PE</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>001eeb60-456e-4817-a52a-6e92d94e25f0</td><td>null</td><td>false</td><td>DISTINCT</td></tr></tbody></table></div>"
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
         "00030cef-1862-4d2b-8e5e-a5c4f8a6cb1c",
         "LL-POL-26474533",
         "POL-26474533",
         "Term Life",
         "1791723.30",
         "4513.05",
         "MONTHLY",
         "2011-02-17",
         "2031-01-10",
         "Suspended",
         "Katherine Cooke",
         "WAIVER",
         "Cindy Cooper",
         "ON",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "ON",
         null,
         null,
         null,
         2026,
         6,
         "00030cef-1862-4d2b-8e5e-a5c4f8a6cb1c",
         null,
         false,
         "DISTINCT"
        ],
        [
         "0007accc-c5c0-429a-83ea-a673045af2b4",
         "LL-POL-93981696",
         "POL-93981696",
         "Whole Life",
         "1115529.08",
         "2902.40",
         "ANNUAL",
         "2013-06-21",
         "2029-02-18",
         "Pending",
         "Jonathan King",
         "WAIVER",
         "Julie Scott",
         "PE",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null,
         2026,
         6,
         "0007accc-c5c0-429a-83ea-a673045af2b4",
         null,
         false,
         "DISTINCT"
        ],
        [
         "0017489d-083b-42c9-96e6-a5871317c79f",
         "GWL-51597224",
         "GWL-51597224",
         "Group Benefits",
         "9011070.60",
         "7287.37",
         "MONTHLY",
         "1996-01-01",
         null,
         "Lapsed",
         null,
         null,
         null,
         "MB",
         null,
         null,
         "2026-06-08T17:44:36.101Z",
         "gwl_policy_admin",
         "7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2",
         null,
         "MISSING",
         null,
         false,
         null,
         "MB",
         null,
         null,
         null,
         2026,
         6,
         "0017489d-083b-42c9-96e6-a5871317c79f",
         null,
         false,
         "DISTINCT"
        ],
        [
         "001b8378-e54a-435e-8307-5bdd98f48ff4",
         "GWL-55103744",
         "GWL-55103744",
         "GIA",
         "4530779.44",
         "118.38",
         "ANNUAL",
         "2006-01-01",
         null,
         "Active",
         null,
         null,
         null,
         "BC",
         null,
         null,
         "2026-06-08T17:44:36.101Z",
         "gwl_policy_admin",
         "7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2",
         null,
         "MISSING",
         null,
         false,
         null,
         "BC",
         null,
         null,
         null,
         2026,
         6,
         "001b8378-e54a-435e-8307-5bdd98f48ff4",
         null,
         false,
         "DISTINCT"
        ],
        [
         "001eeb60-456e-4817-a52a-6e92d94e25f0",
         "LL-POL-77675052",
         "POL-77675052",
         "Term Life",
         "1491499.38",
         "1254.64",
         "ANNUAL",
         "2015-12-23",
         "2032-05-09",
         "Pending",
         "Robert Ross MD",
         null,
         "Christopher Knapp",
         "PE",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null,
         2026,
         6,
         "001eeb60-456e-4817-a52a-6e92d94e25f0",
         null,
         false,
         "DISTINCT"
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
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "legacy_policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "face_amount",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "premium_amount",
         "type": "\"decimal(12,2)\""
        },
        {
         "metadata": "{}",
         "name": "premium_frequency_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "issue_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "expiry_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "policy_status_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "beneficiary_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "rider_codes",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "underwriting_class_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "date_of_birth",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_standardized",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "sin_hashed",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_year",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_month",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "master_customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "identity_match_confidence",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "identity_manual_review_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "identity_resolution_status",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>policy_number</th><th>legacy_policy_number</th><th>customer_id</th><th>product_type_code</th><th>face_amount</th><th>premium_amount</th><th>premium_frequency_code</th><th>issue_date</th><th>expiry_date</th><th>policy_status_code</th><th>beneficiary_id</th><th>rider_codes</th><th>underwriting_class_code</th><th>province</th><th>postal_code</th><th>date_of_birth</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>email_clean</th><th>email_quality_flag</th><th>phone_standardized</th><th>phone_valid_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>sin_hashed</th></tr></thead><tbody><tr><td>LL-POL-00009073</td><td>POL-00009073</td><td>15bdb290-2383-4ab8-b27d-aaa8108b2ebf</td><td>Critical Illness</td><td>1169323.90</td><td>645.36</td><td>MONTHLY</td><td>2008-03-04</td><td>2041-05-26</td><td>Cancelled</td><td>Samantha Curtis</td><td>null</td><td>Stephen Thomas</td><td>QC</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>QC</td><td>null</td><td>null</td><td>null</td></tr><tr><td>LL-POL-00016042</td><td>POL-00016042</td><td>359b80db-b1db-4beb-8505-4547fcfdc063</td><td>Disability</td><td>2538402.21</td><td>2744.01</td><td>ANNUAL</td><td>2011-05-11</td><td>2050-09-17</td><td>Pending</td><td>Angel Mora</td><td>WAIVER,ADB</td><td>James Campbell</td><td>PE</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td></tr><tr><td>LL-POL-00026430</td><td>POL-00026430</td><td>5fa563a7-aacb-4e1a-9498-3acdc5ebe17e</td><td>Critical Illness</td><td>2956065.21</td><td>4805.27</td><td>ANNUAL</td><td>2020-09-18</td><td>2032-04-16</td><td>Cancelled</td><td>Malik Johnson</td><td>WAIVER</td><td>Michael Ford</td><td>QC</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>QC</td><td>null</td><td>null</td><td>null</td></tr><tr><td>LL-POL-00028287</td><td>POL-00028287</td><td>b35cf290-8eba-4817-af0e-c1b7c449ea2d</td><td>Disability</td><td>852225.07</td><td>174.06</td><td>QUARTERLY</td><td>2013-12-12</td><td>2034-10-30</td><td>Pending</td><td>Dustin Bean</td><td>WAIVER</td><td>Holly Garcia</td><td>ON</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>ON</td><td>null</td><td>null</td><td>null</td></tr><tr><td>LL-POL-00028934</td><td>POL-00028934</td><td>86f8cb13-7a97-45b1-95ad-693570e93267</td><td>Critical Illness</td><td>2214444.40</td><td>1158.93</td><td>ANNUAL</td><td>2012-05-29</td><td>2041-03-05</td><td>Suspended</td><td>Jeremy Robinson</td><td>WAIVER,ADB</td><td>Hannah Nelson</td><td>QC</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>QC</td><td>null</td><td>null</td><td>null</td></tr></tbody></table></div>"
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
         "LL-POL-00009073",
         "POL-00009073",
         "15bdb290-2383-4ab8-b27d-aaa8108b2ebf",
         "Critical Illness",
         "1169323.90",
         "645.36",
         "MONTHLY",
         "2008-03-04",
         "2041-05-26",
         "Cancelled",
         "Samantha Curtis",
         null,
         "Stephen Thomas",
         "QC",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "QC",
         null,
         null,
         null
        ],
        [
         "LL-POL-00016042",
         "POL-00016042",
         "359b80db-b1db-4beb-8505-4547fcfdc063",
         "Disability",
         "2538402.21",
         "2744.01",
         "ANNUAL",
         "2011-05-11",
         "2050-09-17",
         "Pending",
         "Angel Mora",
         "WAIVER,ADB",
         "James Campbell",
         "PE",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null
        ],
        [
         "LL-POL-00026430",
         "POL-00026430",
         "5fa563a7-aacb-4e1a-9498-3acdc5ebe17e",
         "Critical Illness",
         "2956065.21",
         "4805.27",
         "ANNUAL",
         "2020-09-18",
         "2032-04-16",
         "Cancelled",
         "Malik Johnson",
         "WAIVER",
         "Michael Ford",
         "QC",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "QC",
         null,
         null,
         null
        ],
        [
         "LL-POL-00028287",
         "POL-00028287",
         "b35cf290-8eba-4817-af0e-c1b7c449ea2d",
         "Disability",
         "852225.07",
         "174.06",
         "QUARTERLY",
         "2013-12-12",
         "2034-10-30",
         "Pending",
         "Dustin Bean",
         "WAIVER",
         "Holly Garcia",
         "ON",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "ON",
         null,
         null,
         null
        ],
        [
         "LL-POL-00028934",
         "POL-00028934",
         "86f8cb13-7a97-45b1-95ad-693570e93267",
         "Critical Illness",
         "2214444.40",
         "1158.93",
         "ANNUAL",
         "2012-05-29",
         "2041-03-05",
         "Suspended",
         "Jeremy Robinson",
         "WAIVER,ADB",
         "Hannah Nelson",
         "QC",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "QC",
         null,
         null,
         null
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
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "legacy_policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "face_amount",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "premium_amount",
         "type": "\"decimal(12,2)\""
        },
        {
         "metadata": "{}",
         "name": "premium_frequency_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "issue_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "expiry_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "policy_status_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "beneficiary_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "rider_codes",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "underwriting_class_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "date_of_birth",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_standardized",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "sin_hashed",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>event_id</th><th>customer_id</th><th>policy_number</th><th>event_type</th><th>event_timestamp</th><th>journey_name</th><th>page_name</th><th>raw_event_json</th><th>_source_system</th><th>_ingested_at</th><th>_batch_id</th><th>event_status</th><th>event_notes</th></tr></thead><tbody><tr><td>0001a11d70f0b450d91b696470381de6e4c78ffb7d7403d48357337002230bd1</td><td>null</td><td>null</td><td>null</td><td>2026-06-08T06:35:05.865Z</td><td>null</td><td>null</td><td>{\"api_metadata\":{\"extracted_at\":\"2026-06-08T06:35:05.865554Z\",\"source_system\":\"AdobeAnalytics_REST\"},\"engagement_details\":{\"conversion\":0,\"page_views\":10,\"product_viewed\":\"TFSA\",\"session_duration_sec\":541},\"interaction_metrics\":{\"browser\":\"Chrome\",\"device\":\"Desktop\",\"geography\":{\"province\":\"NL\"},\"page_url\":\"list/list\",\"referrer\":\"bing\",\"visit_date\":\"2024-09-07\"},\"session_context\":{\"customer_id\":\"d8e6a2d6-099d-4de5-80fb-4ddb40baf2c7\",\"session_id\":\"b431aaa1-afc5-4f41-8164-4c0217c577bb\",\"visitor_id\":\"035b30af-eb26-4854-9f09-41512112f068\"}}</td><td>adobe_analytics.digital_events</td><td>2026-06-09T15:15:12.795Z</td><td>583cf1e6-0424-4ca7-b920-050732524599</td><td>null</td><td>null</td></tr><tr><td>0003478454e836e90987d4eba6b3e4200b9fa281c70fb2ea34311fe3da0205b1</td><td>null</td><td>null</td><td>null</td><td>2026-06-08T06:35:02.519Z</td><td>null</td><td>null</td><td>{\"api_metadata\":{\"extracted_at\":\"2026-06-08T06:35:02.519352Z\",\"source_system\":\"AdobeAnalytics_REST\"},\"engagement_details\":{\"conversion\":1,\"page_views\":20,\"product_viewed\":\"TFSA\",\"session_duration_sec\":2731},\"interaction_metrics\":{\"browser\":\"Chrome\",\"device\":\"Desktop\",\"geography\":{\"province\":\"BC\"},\"page_url\":\"tag/explore\",\"referrer\":\"google\",\"visit_date\":\"2022-01-25\"},\"session_context\":{\"customer_id\":\"5bdd70b8-11aa-45e5-8cf2-bd5922101210\",\"session_id\":\"30fd6649-d3d1-4183-a66a-e7a8b7ac7388\",\"visitor_id\":\"64c21686-611b-452d-80ec-03bd2334398c\"}}</td><td>adobe_analytics.digital_events</td><td>2026-06-09T15:15:12.795Z</td><td>583cf1e6-0424-4ca7-b920-050732524599</td><td>null</td><td>null</td></tr><tr><td>000475345be1f3a77915af3f11235e3ed7a33c8cfda684ca57cd585a919348a9</td><td>bfc45bb5-4da1-448f-b653-9a319b71d9cd</td><td>POL-35268565</td><td>Dental</td><td>2018-06-22T00:00:00.000Z</td><td>null</td><td>null</td><td>null</td><td>my_cl_portal</td><td>2026-06-08T17:47:11.938Z</td><td>3b4619846bf4d1dd015e4d379590244caa4c0065d68237d09bde3a60c3906a34</td><td>Approved</td><td>Culpa culpa mollitia officia occaecati aperiam blanditiis.</td></tr><tr><td>00055090a96d5e6214c2251b431782a6f86ada756ca4abcff02be30d1aa72bc3</td><td>0f9e8223-b08d-4fca-bc7a-487085389187</td><td>POL-58108359</td><td>Death</td><td>2022-01-29T00:00:00.000Z</td><td>null</td><td>null</td><td>null</td><td>my_cl_portal</td><td>2026-06-08T17:47:11.938Z</td><td>3b4619846bf4d1dd015e4d379590244caa4c0065d68237d09bde3a60c3906a34</td><td>Open</td><td>Sequi perferendis repellat accusantium nesciunt reiciendis autem.</td></tr><tr><td>00070a0de6eb95d8b332fc6439b3ff98abf520c2ec5b67610054ea828d68e19c</td><td>c0f0ef02-132f-430d-b8b7-a703d5d5b9a6</td><td>POL-86381423</td><td>Critical Illness</td><td>2017-07-17T00:00:00.000Z</td><td>null</td><td>null</td><td>null</td><td>my_cl_portal</td><td>2026-06-08T17:47:11.938Z</td><td>3b4619846bf4d1dd015e4d379590244caa4c0065d68237d09bde3a60c3906a34</td><td>Approved</td><td>Rerum aperiam odit maxime vitae dolorem.</td></tr></tbody></table></div>"
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
         "0001a11d70f0b450d91b696470381de6e4c78ffb7d7403d48357337002230bd1",
         null,
         null,
         null,
         "2026-06-08T06:35:05.865Z",
         null,
         null,
         "{\"api_metadata\":{\"extracted_at\":\"2026-06-08T06:35:05.865554Z\",\"source_system\":\"AdobeAnalytics_REST\"},\"engagement_details\":{\"conversion\":0,\"page_views\":10,\"product_viewed\":\"TFSA\",\"session_duration_sec\":541},\"interaction_metrics\":{\"browser\":\"Chrome\",\"device\":\"Desktop\",\"geography\":{\"province\":\"NL\"},\"page_url\":\"list/list\",\"referrer\":\"bing\",\"visit_date\":\"2024-09-07\"},\"session_context\":{\"customer_id\":\"d8e6a2d6-099d-4de5-80fb-4ddb40baf2c7\",\"session_id\":\"b431aaa1-afc5-4f41-8164-4c0217c577bb\",\"visitor_id\":\"035b30af-eb26-4854-9f09-41512112f068\"}}",
         "adobe_analytics.digital_events",
         "2026-06-09T15:15:12.795Z",
         "583cf1e6-0424-4ca7-b920-050732524599",
         null,
         null
        ],
        [
         "0003478454e836e90987d4eba6b3e4200b9fa281c70fb2ea34311fe3da0205b1",
         null,
         null,
         null,
         "2026-06-08T06:35:02.519Z",
         null,
         null,
         "{\"api_metadata\":{\"extracted_at\":\"2026-06-08T06:35:02.519352Z\",\"source_system\":\"AdobeAnalytics_REST\"},\"engagement_details\":{\"conversion\":1,\"page_views\":20,\"product_viewed\":\"TFSA\",\"session_duration_sec\":2731},\"interaction_metrics\":{\"browser\":\"Chrome\",\"device\":\"Desktop\",\"geography\":{\"province\":\"BC\"},\"page_url\":\"tag/explore\",\"referrer\":\"google\",\"visit_date\":\"2022-01-25\"},\"session_context\":{\"customer_id\":\"5bdd70b8-11aa-45e5-8cf2-bd5922101210\",\"session_id\":\"30fd6649-d3d1-4183-a66a-e7a8b7ac7388\",\"visitor_id\":\"64c21686-611b-452d-80ec-03bd2334398c\"}}",
         "adobe_analytics.digital_events",
         "2026-06-09T15:15:12.795Z",
         "583cf1e6-0424-4ca7-b920-050732524599",
         null,
         null
        ],
        [
         "000475345be1f3a77915af3f11235e3ed7a33c8cfda684ca57cd585a919348a9",
         "bfc45bb5-4da1-448f-b653-9a319b71d9cd",
         "POL-35268565",
         "Dental",
         "2018-06-22T00:00:00.000Z",
         null,
         null,
         null,
         "my_cl_portal",
         "2026-06-08T17:47:11.938Z",
         "3b4619846bf4d1dd015e4d379590244caa4c0065d68237d09bde3a60c3906a34",
         "Approved",
         "Culpa culpa mollitia officia occaecati aperiam blanditiis."
        ],
        [
         "00055090a96d5e6214c2251b431782a6f86ada756ca4abcff02be30d1aa72bc3",
         "0f9e8223-b08d-4fca-bc7a-487085389187",
         "POL-58108359",
         "Death",
         "2022-01-29T00:00:00.000Z",
         null,
         null,
         null,
         "my_cl_portal",
         "2026-06-08T17:47:11.938Z",
         "3b4619846bf4d1dd015e4d379590244caa4c0065d68237d09bde3a60c3906a34",
         "Open",
         "Sequi perferendis repellat accusantium nesciunt reiciendis autem."
        ],
        [
         "00070a0de6eb95d8b332fc6439b3ff98abf520c2ec5b67610054ea828d68e19c",
         "c0f0ef02-132f-430d-b8b7-a703d5d5b9a6",
         "POL-86381423",
         "Critical Illness",
         "2017-07-17T00:00:00.000Z",
         null,
         null,
         null,
         "my_cl_portal",
         "2026-06-08T17:47:11.938Z",
         "3b4619846bf4d1dd015e4d379590244caa4c0065d68237d09bde3a60c3906a34",
         "Approved",
         "Rerum aperiam odit maxime vitae dolorem."
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
         "name": "event_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "event_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "event_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "journey_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "page_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "raw_event_json",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "event_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "event_notes",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>interaction_id</th><th>customer_id</th><th>agent_id</th><th>call_start_ts</th><th>call_end_ts</th><th>channel</th><th>issue_type</th><th>interaction_status</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>call_duration_minutes</th></tr></thead><tbody><tr><td>INT-92308810</td><td>e4600c23-32ca-4bf0-9d62-cb68d2f3af94</td><td>AGT-3666</td><td>2024-11-02T02:45:32.000Z</td><td>2024-11-02T03:36:18.000Z</td><td>Phone</td><td>Address Change</td><td>Escalated</td><td>2026-06-08T17:46:09.101Z</td><td>avaya_call_centre</td><td>0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7</td><td>50.766666666666666</td></tr><tr><td>INT-37483929</td><td>38f7094d-ba8a-484c-b0af-80e629b2d42e</td><td>AGT-8605</td><td>2024-05-06T12:06:27.000Z</td><td>2024-05-06T12:38:28.000Z</td><td>IVR</td><td>Policy Change</td><td>Resolved</td><td>2026-06-08T17:46:09.101Z</td><td>avaya_call_centre</td><td>0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7</td><td>32.016666666666666</td></tr><tr><td>INT-27151515</td><td>c231946f-c831-4a88-ab54-b5b3d7970bd2</td><td>AGT-9687</td><td>2024-03-13T12:49:55.000Z</td><td>2024-03-13T13:33:04.000Z</td><td>Phone</td><td>Billing</td><td>Pending</td><td>2026-06-08T17:46:09.101Z</td><td>avaya_call_centre</td><td>0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7</td><td>43.15</td></tr><tr><td>INT-99117859</td><td>45bf903f-f2e2-42a5-9ce3-a2fa08e8bd19</td><td>AGT-9015</td><td>2022-01-16T19:34:38.000Z</td><td>2022-01-16T20:01:48.000Z</td><td>IVR</td><td>Coverage Question</td><td>Abandoned</td><td>2026-06-08T17:46:09.101Z</td><td>avaya_call_centre</td><td>0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7</td><td>27.166666666666668</td></tr><tr><td>INT-12743862</td><td>691cc0b6-f450-4052-976b-c60cb80f6dc4</td><td>AGT-1766</td><td>2023-02-22T10:48:49.000Z</td><td>2023-02-22T12:09:13.000Z</td><td>Chat</td><td>Address Change</td><td>Abandoned</td><td>2026-06-08T17:46:09.101Z</td><td>avaya_call_centre</td><td>0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7</td><td>80.4</td></tr></tbody></table></div>"
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
         "INT-92308810",
         "e4600c23-32ca-4bf0-9d62-cb68d2f3af94",
         "AGT-3666",
         "2024-11-02T02:45:32.000Z",
         "2024-11-02T03:36:18.000Z",
         "Phone",
         "Address Change",
         "Escalated",
         "2026-06-08T17:46:09.101Z",
         "avaya_call_centre",
         "0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7",
         50.766666666666666
        ],
        [
         "INT-37483929",
         "38f7094d-ba8a-484c-b0af-80e629b2d42e",
         "AGT-8605",
         "2024-05-06T12:06:27.000Z",
         "2024-05-06T12:38:28.000Z",
         "IVR",
         "Policy Change",
         "Resolved",
         "2026-06-08T17:46:09.101Z",
         "avaya_call_centre",
         "0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7",
         32.016666666666666
        ],
        [
         "INT-27151515",
         "c231946f-c831-4a88-ab54-b5b3d7970bd2",
         "AGT-9687",
         "2024-03-13T12:49:55.000Z",
         "2024-03-13T13:33:04.000Z",
         "Phone",
         "Billing",
         "Pending",
         "2026-06-08T17:46:09.101Z",
         "avaya_call_centre",
         "0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7",
         43.15
        ],
        [
         "INT-99117859",
         "45bf903f-f2e2-42a5-9ce3-a2fa08e8bd19",
         "AGT-9015",
         "2022-01-16T19:34:38.000Z",
         "2022-01-16T20:01:48.000Z",
         "IVR",
         "Coverage Question",
         "Abandoned",
         "2026-06-08T17:46:09.101Z",
         "avaya_call_centre",
         "0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7",
         27.166666666666668
        ],
        [
         "INT-12743862",
         "691cc0b6-f450-4052-976b-c60cb80f6dc4",
         "AGT-1766",
         "2023-02-22T10:48:49.000Z",
         "2023-02-22T12:09:13.000Z",
         "Chat",
         "Address Change",
         "Abandoned",
         "2026-06-08T17:46:09.101Z",
         "avaya_call_centre",
         "0879c4fcba8fb9acb6f6225df14d1c7635f3d3481424a9ac931163f0cb3dd3e7",
         80.4
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
         "name": "interaction_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "agent_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "call_start_ts",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "call_end_ts",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "channel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "issue_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "interaction_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "call_duration_minutes",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>member_id</th><th>plan_id</th><th>certificate_number</th><th>employer_name</th><th>first_name</th><th>last_name</th><th>date_of_birth</th><th>province</th><th>effective_date</th><th>termination_date</th><th>coverage_type_codes_enrolled</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>email_clean</th><th>email_quality_flag</th><th>phone_standardized</th><th>phone_valid_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>sin_hashed</th></tr></thead><tbody><tr><td>MBR-24490319</td><td>GRP-0000</td><td>CERT-MBR-24490319</td><td>Garza-Hayes</td><td>John</td><td>Tapia</td><td>1963-12-01</td><td>MB</td><td>2020-04-09</td><td>2020-11-20</td><td>Couple,GRP_DENTAL</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>MB</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-98718349</td><td>GRP-0000</td><td>CERT-MBR-98718349</td><td>Williamson-Johnson</td><td>Christopher</td><td>Mccarthy</td><td>1968-04-05</td><td>PE</td><td>2023-08-19</td><td>null</td><td>Family,GRP_DENTAL</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-18042706</td><td>GRP-0001</td><td>CERT-MBR-18042706</td><td>Miller Inc</td><td>Sydney</td><td>Russell</td><td>1970-01-13</td><td>ON</td><td>2018-06-10</td><td>null</td><td>Single,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>ON</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-77133618</td><td>GRP-0001</td><td>CERT-MBR-77133618</td><td>Dean-Sims</td><td>Thomas</td><td>Sosa</td><td>1977-08-26</td><td>NB</td><td>2022-04-28</td><td>2021-11-28</td><td>Couple,GRP_DENTAL,GRP_VISION,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>NB</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-88555522</td><td>GRP-0001</td><td>CERT-MBR-88555522</td><td>Lee Ltd</td><td>Brian</td><td>Gonzales</td><td>2000-05-10</td><td>NB</td><td>2021-06-01</td><td>null</td><td>Single,GRP_DENTAL</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>NB</td><td>null</td><td>null</td><td>null</td></tr></tbody></table></div>"
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
         "MBR-24490319",
         "GRP-0000",
         "CERT-MBR-24490319",
         "Garza-Hayes",
         "John",
         "Tapia",
         "1963-12-01",
         "MB",
         "2020-04-09",
         "2020-11-20",
         "Couple,GRP_DENTAL",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "MB",
         null,
         null,
         null
        ],
        [
         "MBR-98718349",
         "GRP-0000",
         "CERT-MBR-98718349",
         "Williamson-Johnson",
         "Christopher",
         "Mccarthy",
         "1968-04-05",
         "PE",
         "2023-08-19",
         null,
         "Family,GRP_DENTAL",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null
        ],
        [
         "MBR-18042706",
         "GRP-0001",
         "CERT-MBR-18042706",
         "Miller Inc",
         "Sydney",
         "Russell",
         "1970-01-13",
         "ON",
         "2018-06-10",
         null,
         "Single,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "ON",
         null,
         null,
         null
        ],
        [
         "MBR-77133618",
         "GRP-0001",
         "CERT-MBR-77133618",
         "Dean-Sims",
         "Thomas",
         "Sosa",
         "1977-08-26",
         "NB",
         "2022-04-28",
         "2021-11-28",
         "Couple,GRP_DENTAL,GRP_VISION,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "NB",
         null,
         null,
         null
        ],
        [
         "MBR-88555522",
         "GRP-0001",
         "CERT-MBR-88555522",
         "Lee Ltd",
         "Brian",
         "Gonzales",
         "2000-05-10",
         "NB",
         "2021-06-01",
         null,
         "Single,GRP_DENTAL",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "NB",
         null,
         null,
         null
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
         "name": "member_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "plan_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "certificate_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "employer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "first_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "last_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "date_of_birth",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "province",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "effective_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "termination_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "coverage_type_codes_enrolled",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_standardized",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "sin_hashed",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>member_id</th><th>plan_id</th><th>certificate_number</th><th>employer_name</th><th>first_name</th><th>last_name</th><th>date_of_birth</th><th>province</th><th>effective_date</th><th>termination_date</th><th>coverage_type_codes_enrolled</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>email_clean</th><th>email_quality_flag</th><th>phone_standardized</th><th>phone_valid_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>sin_hashed</th></tr></thead><tbody><tr><td>MBR-00003362</td><td>GRP-8103</td><td>CERT-MBR-00003362</td><td>Owens, Kim and Wyatt</td><td>Brett</td><td>Salazar</td><td>1980-08-02</td><td>NB</td><td>2018-03-27</td><td>null</td><td>Couple</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>NB</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-00005580</td><td>GRP-1891</td><td>CERT-MBR-00005580</td><td>Norris Inc</td><td>Zachary</td><td>Humphrey</td><td>1971-03-23</td><td>PE</td><td>2019-07-22</td><td>null</td><td>Single,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-00006623</td><td>GRP-3172</td><td>CERT-MBR-00006623</td><td>Christensen, Henry and Miller</td><td>Sonya</td><td>Grant</td><td>1986-11-01</td><td>NL</td><td>2016-05-10</td><td>2021-10-27</td><td>Single,GRP_VISION,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>NL</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-00007270</td><td>GRP-2351</td><td>CERT-MBR-00007270</td><td>Sharp, Leblanc and Murray</td><td>Taylor</td><td>Castillo</td><td>1974-10-17</td><td>SK</td><td>2023-08-22</td><td>2023-09-23</td><td>Family,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>SK</td><td>null</td><td>null</td><td>null</td></tr><tr><td>MBR-00008677</td><td>GRP-5101</td><td>CERT-MBR-00008677</td><td>Ross PLC</td><td>Robin</td><td>Whitaker</td><td>1976-10-12</td><td>PE</td><td>2017-09-03</td><td>2024-01-21</td><td>Couple,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td></tr></tbody></table></div>"
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
         "MBR-00003362",
         "GRP-8103",
         "CERT-MBR-00003362",
         "Owens, Kim and Wyatt",
         "Brett",
         "Salazar",
         "1980-08-02",
         "NB",
         "2018-03-27",
         null,
         "Couple",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "NB",
         null,
         null,
         null
        ],
        [
         "MBR-00005580",
         "GRP-1891",
         "CERT-MBR-00005580",
         "Norris Inc",
         "Zachary",
         "Humphrey",
         "1971-03-23",
         "PE",
         "2019-07-22",
         null,
         "Single,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null
        ],
        [
         "MBR-00006623",
         "GRP-3172",
         "CERT-MBR-00006623",
         "Christensen, Henry and Miller",
         "Sonya",
         "Grant",
         "1986-11-01",
         "NL",
         "2016-05-10",
         "2021-10-27",
         "Single,GRP_VISION,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "NL",
         null,
         null,
         null
        ],
        [
         "MBR-00007270",
         "GRP-2351",
         "CERT-MBR-00007270",
         "Sharp, Leblanc and Murray",
         "Taylor",
         "Castillo",
         "1974-10-17",
         "SK",
         "2023-08-22",
         "2023-09-23",
         "Family,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "SK",
         null,
         null,
         null
        ],
        [
         "MBR-00008677",
         "GRP-5101",
         "CERT-MBR-00008677",
         "Ross PLC",
         "Robin",
         "Whitaker",
         "1976-10-12",
         "PE",
         "2017-09-03",
         "2024-01-21",
         "Couple,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null
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
         "name": "member_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "plan_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "certificate_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "employer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "first_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "last_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "date_of_birth",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "province",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "effective_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "termination_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "coverage_type_codes_enrolled",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_standardized",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "sin_hashed",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>assignment_id</th><th>advisor_id</th><th>customer_id</th><th>assignment_start_date</th><th>assignment_end_date</th><th>region</th><th>channel</th><th>advisor_assignment_status</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>policy_number</th><th>product_type_code</th></tr></thead><tbody><tr><td>ASN-11669735</td><td>ADV-0000</td><td>50a6ffb0-2837-42a2-bc7e-c067fc0c650b</td><td>2019-01-25</td><td>2020-04-09</td><td>Quebec</td><td>Online</td><td>Active</td><td>2026-06-08T17:46:51.408Z</td><td>f55_advisor</td><td>57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7</td><td>null</td><td>null</td></tr><tr><td>ASN-17716206</td><td>ADV-0000</td><td>34dfb3e1-e2d8-4252-99b3-f9121e7236d0</td><td>2021-09-16</td><td>2022-07-10</td><td>Quebec</td><td>Online</td><td>Active</td><td>2026-06-08T17:46:51.408Z</td><td>f55_advisor</td><td>57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7</td><td>null</td><td>null</td></tr><tr><td>ASN-18367010</td><td>ADV-0000</td><td>4aa9ce66-0946-4fa4-a7c4-192358c2bdd3</td><td>2023-02-03</td><td>2024-07-15</td><td>East</td><td>Agent</td><td>Inactive</td><td>2026-06-08T17:46:51.408Z</td><td>f55_advisor</td><td>57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7</td><td>null</td><td>null</td></tr><tr><td>ASN-21327094</td><td>ADV-0000</td><td>8fa1ee93-ccc9-45c7-8fbe-d0eb147fdb94</td><td>2024-12-16</td><td>2025-10-12</td><td>East</td><td>Online</td><td>Inactive</td><td>2026-06-08T17:46:51.408Z</td><td>f55_advisor</td><td>57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7</td><td>null</td><td>null</td></tr><tr><td>ASN-25732783</td><td>ADV-0000</td><td>beef7c99-d90e-4512-ac16-4262dbfe17ae</td><td>2018-09-18</td><td>2020-02-03</td><td>East</td><td>Employer Group</td><td>Transferred</td><td>2026-06-08T17:46:51.408Z</td><td>f55_advisor</td><td>57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7</td><td>null</td><td>null</td></tr></tbody></table></div>"
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
         "ASN-11669735",
         "ADV-0000",
         "50a6ffb0-2837-42a2-bc7e-c067fc0c650b",
         "2019-01-25",
         "2020-04-09",
         "Quebec",
         "Online",
         "Active",
         "2026-06-08T17:46:51.408Z",
         "f55_advisor",
         "57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7",
         null,
         null
        ],
        [
         "ASN-17716206",
         "ADV-0000",
         "34dfb3e1-e2d8-4252-99b3-f9121e7236d0",
         "2021-09-16",
         "2022-07-10",
         "Quebec",
         "Online",
         "Active",
         "2026-06-08T17:46:51.408Z",
         "f55_advisor",
         "57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7",
         null,
         null
        ],
        [
         "ASN-18367010",
         "ADV-0000",
         "4aa9ce66-0946-4fa4-a7c4-192358c2bdd3",
         "2023-02-03",
         "2024-07-15",
         "East",
         "Agent",
         "Inactive",
         "2026-06-08T17:46:51.408Z",
         "f55_advisor",
         "57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7",
         null,
         null
        ],
        [
         "ASN-21327094",
         "ADV-0000",
         "8fa1ee93-ccc9-45c7-8fbe-d0eb147fdb94",
         "2024-12-16",
         "2025-10-12",
         "East",
         "Online",
         "Inactive",
         "2026-06-08T17:46:51.408Z",
         "f55_advisor",
         "57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7",
         null,
         null
        ],
        [
         "ASN-25732783",
         "ADV-0000",
         "beef7c99-d90e-4512-ac16-4262dbfe17ae",
         "2018-09-18",
         "2020-02-03",
         "East",
         "Employer Group",
         "Transferred",
         "2026-06-08T17:46:51.408Z",
         "f55_advisor",
         "57942a03fa479f032c9925844daec5cf5cbc33315e7ed8a2e6f9b791ca39a3f7",
         null,
         null
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
         "name": "assignment_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "advisor_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "assignment_start_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "assignment_end_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "region",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "channel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "advisor_assignment_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>contract_number</th><th>customer_id</th><th>product_type_code</th><th>fund_code</th><th>units</th><th>nav</th><th>market_value</th><th>purchase_date</th><th>currency</th><th>benchmark</th><th>ytd_return_pct</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th></tr></thead><tbody><tr><td>INV-00000367</td><td>a193313c-d3eb-456c-ab7a-f5a8a08aeb6e</td><td>GIC</td><td>FND-7702</td><td>4838.6617</td><td>81.0918</td><td>425675.29</td><td>2013-03-05</td><td>CAD</td><td>TSX</td><td>8.05</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td></tr><tr><td>INV-00000718</td><td>b8cd7a1e-404f-4cd7-b001-6d3fa2aa286a</td><td>GIC</td><td>FND-0605</td><td>5890.2356</td><td>146.0189</td><td>220181.37</td><td>2020-11-06</td><td>CAD</td><td>MSCI</td><td>19.59</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td></tr><tr><td>INV-00001177</td><td>695c71c6-3f81-4c3a-b309-e03b332516e4</td><td>GIC</td><td>FND-0825</td><td>8228.0653</td><td>124.1425</td><td>783219.01</td><td>2015-05-19</td><td>CAD</td><td>FTSE</td><td>13.77</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td></tr><tr><td>INV-00001249</td><td>fe72380c-acfb-45fc-a346-0b171280d07a</td><td>Equity</td><td>FND-5523</td><td>5379.2899</td><td>89.8348</td><td>71629.85</td><td>2014-03-24</td><td>CAD</td><td>S&P500</td><td>-7.97</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td></tr><tr><td>INV-00005429</td><td>91e46b68-cecd-4eae-96f9-10e4c5cffb5d</td><td>Money Market</td><td>FND-9627</td><td>7251.7386</td><td>154.4084</td><td>658664.23</td><td>2021-01-22</td><td>CAD</td><td>S&P500</td><td>18.85</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td></tr></tbody></table></div>"
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
         "INV-00000367",
         "a193313c-d3eb-456c-ab7a-f5a8a08aeb6e",
         "GIC",
         "FND-7702",
         4838.6617,
         81.0918,
         425675.29,
         "2013-03-05",
         "CAD",
         "TSX",
         8.05,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804"
        ],
        [
         "INV-00000718",
         "b8cd7a1e-404f-4cd7-b001-6d3fa2aa286a",
         "GIC",
         "FND-0605",
         5890.2356,
         146.0189,
         220181.37,
         "2020-11-06",
         "CAD",
         "MSCI",
         19.59,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804"
        ],
        [
         "INV-00001177",
         "695c71c6-3f81-4c3a-b309-e03b332516e4",
         "GIC",
         "FND-0825",
         8228.0653,
         124.1425,
         783219.01,
         "2015-05-19",
         "CAD",
         "FTSE",
         13.77,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804"
        ],
        [
         "INV-00001249",
         "fe72380c-acfb-45fc-a346-0b171280d07a",
         "Equity",
         "FND-5523",
         5379.2899,
         89.8348,
         71629.85,
         "2014-03-24",
         "CAD",
         "S&P500",
         -7.97,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804"
        ],
        [
         "INV-00005429",
         "91e46b68-cecd-4eae-96f9-10e4c5cffb5d",
         "Money Market",
         "FND-9627",
         7251.7386,
         154.4084,
         658664.23,
         "2021-01-22",
         "CAD",
         "S&P500",
         18.85,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804"
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
         "name": "contract_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "fund_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "units",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "nav",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "market_value",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "purchase_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "currency",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "benchmark",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ytd_return_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>member_id</th><th>plan_id</th><th>employer_id</th><th>retirement_date</th><th>contribution_amount</th><th>vesting_status</th><th>member_status</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th></tr></thead><tbody><tr><td>GRM-00000011</td><td>GRP-6244</td><td>EMP-793236</td><td>2054-01-17</td><td>7825.30</td><td>Partially Vested</td><td>Retired</td><td>2026-06-08T17:47:53.638Z</td><td>group_retirement</td><td>526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f</td></tr><tr><td>GRM-00000438</td><td>GRP-3749</td><td>EMP-009090</td><td>2054-03-24</td><td>15794.18</td><td>Not Vested</td><td>Active</td><td>2026-06-08T17:47:53.638Z</td><td>group_retirement</td><td>526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f</td></tr><tr><td>GRM-00001242</td><td>GRP-5310</td><td>EMP-123617</td><td>2051-04-20</td><td>18832.64</td><td>Partially Vested</td><td>Deferred</td><td>2026-06-08T17:47:53.638Z</td><td>group_retirement</td><td>526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f</td></tr><tr><td>GRM-00002016</td><td>GRP-2307</td><td>EMP-216022</td><td>2037-12-03</td><td>20963.49</td><td>Not Vested</td><td>Retired</td><td>2026-06-08T17:47:53.638Z</td><td>group_retirement</td><td>526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f</td></tr><tr><td>GRM-00002246</td><td>GRP-8114</td><td>EMP-564967</td><td>2039-12-22</td><td>3923.21</td><td>Not Vested</td><td>Terminated</td><td>2026-06-08T17:47:53.638Z</td><td>group_retirement</td><td>526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f</td></tr></tbody></table></div>"
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
         "GRM-00000011",
         "GRP-6244",
         "EMP-793236",
         "2054-01-17",
         "7825.30",
         "Partially Vested",
         "Retired",
         "2026-06-08T17:47:53.638Z",
         "group_retirement",
         "526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f"
        ],
        [
         "GRM-00000438",
         "GRP-3749",
         "EMP-009090",
         "2054-03-24",
         "15794.18",
         "Not Vested",
         "Active",
         "2026-06-08T17:47:53.638Z",
         "group_retirement",
         "526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f"
        ],
        [
         "GRM-00001242",
         "GRP-5310",
         "EMP-123617",
         "2051-04-20",
         "18832.64",
         "Partially Vested",
         "Deferred",
         "2026-06-08T17:47:53.638Z",
         "group_retirement",
         "526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f"
        ],
        [
         "GRM-00002016",
         "GRP-2307",
         "EMP-216022",
         "2037-12-03",
         "20963.49",
         "Not Vested",
         "Retired",
         "2026-06-08T17:47:53.638Z",
         "group_retirement",
         "526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f"
        ],
        [
         "GRM-00002246",
         "GRP-8114",
         "EMP-564967",
         "2039-12-22",
         "3923.21",
         "Not Vested",
         "Terminated",
         "2026-06-08T17:47:53.638Z",
         "group_retirement",
         "526999af4e7d8661fcc3047683d566fcd7ac1087d9bba6fd316fe47e18f2b45f"
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
         "name": "member_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "plan_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "employer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "retirement_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "contribution_amount",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "vesting_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "member_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>treaty_id</th><th>policy_number</th><th>reinsurer_name</th><th>ceded_amount</th><th>retained_amount</th><th>premium_ceded</th><th>claim_recovered</th><th>effective_date</th><th>expiry_date</th><th>product_type_code</th><th>policy_status_code</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th></tr></thead><tbody><tr><td>TRT-0000</td><td>POL-29340818</td><td>Munich Re</td><td>3383470.26</td><td>2778422.84</td><td>5478.67</td><td>36130.05</td><td>2013-01-24</td><td>2029-07-08</td><td>Group Benefits</td><td>Active</td><td>2026-06-08T17:48:14.845Z</td><td>reinsurance</td><td>e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833</td></tr><tr><td>TRT-0001</td><td>POL-45385170</td><td>Munich Re</td><td>2074146.41</td><td>3780483.44</td><td>8499.78</td><td>387538.72</td><td>2019-01-08</td><td>2024-03-01</td><td>GIA</td><td>Terminated</td><td>2026-06-08T17:48:14.845Z</td><td>reinsurance</td><td>e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833</td></tr><tr><td>TRT-0002</td><td>POL-82525460</td><td>Munich Re</td><td>3998678.46</td><td>4934026.47</td><td>42479.99</td><td>48093.14</td><td>2021-07-25</td><td>2028-08-14</td><td>LTC</td><td>Active</td><td>2026-06-08T17:48:14.845Z</td><td>reinsurance</td><td>e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833</td></tr><tr><td>TRT-0003</td><td>POL-89731890</td><td>Munich Re</td><td>1671117.91</td><td>3482065.59</td><td>49119.33</td><td>369576.82</td><td>2012-06-02</td><td>2035-10-12</td><td>LTC</td><td>Active</td><td>2026-06-08T17:48:14.845Z</td><td>reinsurance</td><td>e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833</td></tr><tr><td>TRT-0004</td><td>POL-32406110</td><td>Gen Re</td><td>3464802.70</td><td>4630049.97</td><td>35979.94</td><td>151392.02</td><td>2010-02-07</td><td>2032-08-08</td><td>LTC</td><td>Expired</td><td>2026-06-08T17:48:14.845Z</td><td>reinsurance</td><td>e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833</td></tr></tbody></table></div>"
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
         "TRT-0000",
         "POL-29340818",
         "Munich Re",
         "3383470.26",
         "2778422.84",
         "5478.67",
         "36130.05",
         "2013-01-24",
         "2029-07-08",
         "Group Benefits",
         "Active",
         "2026-06-08T17:48:14.845Z",
         "reinsurance",
         "e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833"
        ],
        [
         "TRT-0001",
         "POL-45385170",
         "Munich Re",
         "2074146.41",
         "3780483.44",
         "8499.78",
         "387538.72",
         "2019-01-08",
         "2024-03-01",
         "GIA",
         "Terminated",
         "2026-06-08T17:48:14.845Z",
         "reinsurance",
         "e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833"
        ],
        [
         "TRT-0002",
         "POL-82525460",
         "Munich Re",
         "3998678.46",
         "4934026.47",
         "42479.99",
         "48093.14",
         "2021-07-25",
         "2028-08-14",
         "LTC",
         "Active",
         "2026-06-08T17:48:14.845Z",
         "reinsurance",
         "e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833"
        ],
        [
         "TRT-0003",
         "POL-89731890",
         "Munich Re",
         "1671117.91",
         "3482065.59",
         "49119.33",
         "369576.82",
         "2012-06-02",
         "2035-10-12",
         "LTC",
         "Active",
         "2026-06-08T17:48:14.845Z",
         "reinsurance",
         "e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833"
        ],
        [
         "TRT-0004",
         "POL-32406110",
         "Gen Re",
         "3464802.70",
         "4630049.97",
         "35979.94",
         "151392.02",
         "2010-02-07",
         "2032-08-08",
         "LTC",
         "Expired",
         "2026-06-08T17:48:14.845Z",
         "reinsurance",
         "e12c5fa6ca2555ac72b8837a13765fc0cee723de8ded714d7f48f8d548027833"
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
         "name": "treaty_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "reinsurer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ceded_amount",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "retained_amount",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "premium_ceded",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "claim_recovered",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "effective_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "expiry_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_status_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>legacy_code</th><th>source_system</th><th>canonical_code</th><th>product_category</th></tr></thead><tbody><tr><td>TFSA</td><td>gwl_policy_admin</td><td>TFSA</td><td>UNKNOWN</td></tr><tr><td>Whole Life</td><td>gwl_policy_admin</td><td>WHOLELIFE</td><td>WHOLE_LIFE</td></tr><tr><td>Disability</td><td>gwl_policy_admin</td><td>D</td><td>UNKNOWN</td></tr><tr><td>Group Benefits</td><td>gwl_policy_admin</td><td>GB</td><td>UNKNOWN</td></tr><tr><td>Critical Illness</td><td>ll_policy_admin</td><td>CI</td><td>UNKNOWN</td></tr></tbody></table></div>"
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
         "TFSA",
         "gwl_policy_admin",
         "TFSA",
         "UNKNOWN"
        ],
        [
         "Whole Life",
         "gwl_policy_admin",
         "WHOLELIFE",
         "WHOLE_LIFE"
        ],
        [
         "Disability",
         "gwl_policy_admin",
         "D",
         "UNKNOWN"
        ],
        [
         "Group Benefits",
         "gwl_policy_admin",
         "GB",
         "UNKNOWN"
        ],
        [
         "Critical Illness",
         "ll_policy_admin",
         "CI",
         "UNKNOWN"
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
         "name": "legacy_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "canonical_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_category",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>legacy_code</th><th>source_system</th><th>canonical_status_code</th></tr></thead><tbody><tr><td>Lapsed</td><td>ll_policy_admin</td><td>L</td></tr><tr><td>Pending</td><td>ll_policy_admin</td><td>P</td></tr><tr><td>Suspended</td><td>ll_policy_admin</td><td>S</td></tr><tr><td>Cancelled</td><td>gwl_policy_admin</td><td>C</td></tr><tr><td>Cancelled</td><td>ll_policy_admin</td><td>C</td></tr></tbody></table></div>"
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
         "Lapsed",
         "ll_policy_admin",
         "L"
        ],
        [
         "Pending",
         "ll_policy_admin",
         "P"
        ],
        [
         "Suspended",
         "ll_policy_admin",
         "S"
        ],
        [
         "Cancelled",
         "gwl_policy_admin",
         "C"
        ],
        [
         "Cancelled",
         "ll_policy_admin",
         "C"
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
         "name": "legacy_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "canonical_status_code",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>rider_code</th><th>rider_description</th><th>rider_category</th></tr></thead><tbody><tr><td>ADB</td><td>null</td><td>UNKNOWN</td></tr><tr><td>WAIVER</td><td>null</td><td>UNKNOWN</td></tr></tbody></table></div>"
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
         "ADB",
         null,
         "UNKNOWN"
        ],
        [
         "WAIVER",
         null,
         "UNKNOWN"
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
         "name": "rider_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "rider_description",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "rider_category",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>premium_frequency_code</th><th>customer_id</th><th>policy_number</th><th>legacy_policy_number</th><th>product_type_code</th><th>face_amount</th><th>premium_amount</th><th>issue_date</th><th>expiry_date</th><th>policy_status_code</th><th>beneficiary_id</th><th>rider_codes</th><th>underwriting_class_code</th><th>province</th><th>postal_code</th><th>date_of_birth</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>email_clean</th><th>email_quality_flag</th><th>phone_standardized</th><th>phone_valid_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>sin_hashed</th><th>_ingested_year</th><th>_ingested_month</th><th>master_customer_id</th><th>identity_match_confidence</th><th>identity_manual_review_flag</th><th>identity_resolution_status</th><th>annualised_premium</th><th>product_category</th><th>product_type_code_canonical</th><th>policy_status_canonical</th><th>policy_tenure_days</th><th>term_expiry_days_remaining</th><th>term_expiring_90d_flag</th><th>churn_risk_signal</th></tr></thead><tbody><tr><td>ANNUAL</td><td>00014ae9-2750-44dd-9095-694f3c0fe34c</td><td>GWL-10194089</td><td>GWL-10194089</td><td>Annuity</td><td>9942356.57</td><td>3064.31</td><td>2005-01-01</td><td>null</td><td>Pending</td><td>null</td><td>null</td><td>null</td><td>SK</td><td>null</td><td>null</td><td>2026-06-08T17:44:36.101Z</td><td>gwl_policy_admin</td><td>7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>SK</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>00014ae9-2750-44dd-9095-694f3c0fe34c</td><td>null</td><td>false</td><td>DISTINCT</td><td>3064.31</td><td>UNKNOWN</td><td>A</td><td>P</td><td>7828</td><td>null</td><td>false</td><td>LOW</td></tr><tr><td>ANNUAL</td><td>00065552-d8ba-40a8-8578-c731c93ccfd8</td><td>LL-POL-86347127</td><td>POL-86347127</td><td>GIA</td><td>3339727.26</td><td>1723.79</td><td>2013-01-31</td><td>2035-02-06</td><td>Suspended</td><td>Lisa Lewis</td><td>null</td><td>Claudia Wade</td><td>NS</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>NS</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>00065552-d8ba-40a8-8578-c731c93ccfd8</td><td>null</td><td>false</td><td>DISTINCT</td><td>1723.79</td><td>UNKNOWN</td><td>GIA</td><td>S</td><td>4876</td><td>3165</td><td>false</td><td>LOW</td></tr><tr><td>MONTHLY</td><td>0007178d-0b25-4a20-867f-ce2fecf84700</td><td>GWL-10605163</td><td>GWL-10605163</td><td>Critical Illness</td><td>3408728.10</td><td>6192.73</td><td>2009-01-01</td><td>null</td><td>Active</td><td>null</td><td>null</td><td>null</td><td>MB</td><td>null</td><td>null</td><td>2026-06-08T17:44:36.101Z</td><td>gwl_policy_admin</td><td>7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>MB</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>0007178d-0b25-4a20-867f-ce2fecf84700</td><td>null</td><td>false</td><td>DISTINCT</td><td>74312.76</td><td>UNKNOWN</td><td>CI</td><td>A</td><td>6367</td><td>null</td><td>false</td><td>LOW</td></tr><tr><td>ANNUAL</td><td>0007accc-c5c0-429a-83ea-a673045af2b4</td><td>LL-POL-93981696</td><td>POL-93981696</td><td>Whole Life</td><td>1115529.08</td><td>2902.40</td><td>2013-06-21</td><td>2029-02-18</td><td>Pending</td><td>Jonathan King</td><td>WAIVER</td><td>Julie Scott</td><td>PE</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>0007accc-c5c0-429a-83ea-a673045af2b4</td><td>null</td><td>false</td><td>DISTINCT</td><td>2902.40</td><td>WHOLE_LIFE</td><td>WHOLELIFE</td><td>P</td><td>4735</td><td>986</td><td>false</td><td>LOW</td></tr><tr><td>ANNUAL</td><td>000be53d-9b88-4db9-ba9c-8bb78acb8329</td><td>GWL-01984411</td><td>GWL-01984411</td><td>LTC</td><td>3173043.08</td><td>2929.12</td><td>1981-01-01</td><td>null</td><td>Suspended</td><td>null</td><td>null</td><td>null</td><td>BC</td><td>null</td><td>null</td><td>2026-06-08T17:44:36.101Z</td><td>gwl_policy_admin</td><td>7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>BC</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>000be53d-9b88-4db9-ba9c-8bb78acb8329</td><td>null</td><td>false</td><td>DISTINCT</td><td>2929.12</td><td>UNKNOWN</td><td>LTC</td><td>S</td><td>16594</td><td>null</td><td>false</td><td>LOW</td></tr></tbody></table></div>"
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
         "ANNUAL",
         "00014ae9-2750-44dd-9095-694f3c0fe34c",
         "GWL-10194089",
         "GWL-10194089",
         "Annuity",
         "9942356.57",
         "3064.31",
         "2005-01-01",
         null,
         "Pending",
         null,
         null,
         null,
         "SK",
         null,
         null,
         "2026-06-08T17:44:36.101Z",
         "gwl_policy_admin",
         "7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2",
         null,
         "MISSING",
         null,
         false,
         null,
         "SK",
         null,
         null,
         null,
         2026,
         6,
         "00014ae9-2750-44dd-9095-694f3c0fe34c",
         null,
         false,
         "DISTINCT",
         "3064.31",
         "UNKNOWN",
         "A",
         "P",
         7828,
         null,
         false,
         "LOW"
        ],
        [
         "ANNUAL",
         "00065552-d8ba-40a8-8578-c731c93ccfd8",
         "LL-POL-86347127",
         "POL-86347127",
         "GIA",
         "3339727.26",
         "1723.79",
         "2013-01-31",
         "2035-02-06",
         "Suspended",
         "Lisa Lewis",
         null,
         "Claudia Wade",
         "NS",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "NS",
         null,
         null,
         null,
         2026,
         6,
         "00065552-d8ba-40a8-8578-c731c93ccfd8",
         null,
         false,
         "DISTINCT",
         "1723.79",
         "UNKNOWN",
         "GIA",
         "S",
         4876,
         3165,
         false,
         "LOW"
        ],
        [
         "MONTHLY",
         "0007178d-0b25-4a20-867f-ce2fecf84700",
         "GWL-10605163",
         "GWL-10605163",
         "Critical Illness",
         "3408728.10",
         "6192.73",
         "2009-01-01",
         null,
         "Active",
         null,
         null,
         null,
         "MB",
         null,
         null,
         "2026-06-08T17:44:36.101Z",
         "gwl_policy_admin",
         "7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2",
         null,
         "MISSING",
         null,
         false,
         null,
         "MB",
         null,
         null,
         null,
         2026,
         6,
         "0007178d-0b25-4a20-867f-ce2fecf84700",
         null,
         false,
         "DISTINCT",
         "74312.76",
         "UNKNOWN",
         "CI",
         "A",
         6367,
         null,
         false,
         "LOW"
        ],
        [
         "ANNUAL",
         "0007accc-c5c0-429a-83ea-a673045af2b4",
         "LL-POL-93981696",
         "POL-93981696",
         "Whole Life",
         "1115529.08",
         "2902.40",
         "2013-06-21",
         "2029-02-18",
         "Pending",
         "Jonathan King",
         "WAIVER",
         "Julie Scott",
         "PE",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null,
         2026,
         6,
         "0007accc-c5c0-429a-83ea-a673045af2b4",
         null,
         false,
         "DISTINCT",
         "2902.40",
         "WHOLE_LIFE",
         "WHOLELIFE",
         "P",
         4735,
         986,
         false,
         "LOW"
        ],
        [
         "ANNUAL",
         "000be53d-9b88-4db9-ba9c-8bb78acb8329",
         "GWL-01984411",
         "GWL-01984411",
         "LTC",
         "3173043.08",
         "2929.12",
         "1981-01-01",
         null,
         "Suspended",
         null,
         null,
         null,
         "BC",
         null,
         null,
         "2026-06-08T17:44:36.101Z",
         "gwl_policy_admin",
         "7cf08ca0954359e585a13e329baa1399227d4652407b505193cd9e33f1e8ffd2",
         null,
         "MISSING",
         null,
         false,
         null,
         "BC",
         null,
         null,
         null,
         2026,
         6,
         "000be53d-9b88-4db9-ba9c-8bb78acb8329",
         null,
         false,
         "DISTINCT",
         "2929.12",
         "UNKNOWN",
         "LTC",
         "S",
         16594,
         null,
         false,
         "LOW"
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
         "name": "premium_frequency_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "legacy_policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "face_amount",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "premium_amount",
         "type": "\"decimal(12,2)\""
        },
        {
         "metadata": "{}",
         "name": "issue_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "expiry_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "policy_status_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "beneficiary_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "rider_codes",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "underwriting_class_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "date_of_birth",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_standardized",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "sin_hashed",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_year",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_month",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "master_customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "identity_match_confidence",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "identity_manual_review_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "identity_resolution_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "annualised_premium",
         "type": "\"decimal(12,2)\""
        },
        {
         "metadata": "{}",
         "name": "product_category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code_canonical",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_status_canonical",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_tenure_days",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "term_expiry_days_remaining",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "term_expiring_90d_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "churn_risk_signal",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>rider_code</th><th>premium_frequency_code</th><th>customer_id</th><th>policy_number</th><th>legacy_policy_number</th><th>product_type_code</th><th>face_amount</th><th>premium_amount</th><th>issue_date</th><th>expiry_date</th><th>policy_status_code</th><th>beneficiary_id</th><th>underwriting_class_code</th><th>province</th><th>postal_code</th><th>date_of_birth</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>email_clean</th><th>email_quality_flag</th><th>phone_standardized</th><th>phone_valid_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>sin_hashed</th><th>_ingested_year</th><th>_ingested_month</th><th>master_customer_id</th><th>identity_match_confidence</th><th>identity_manual_review_flag</th><th>identity_resolution_status</th><th>annualised_premium</th><th>product_category</th><th>product_type_code_canonical</th><th>policy_status_canonical</th><th>policy_tenure_days</th><th>term_expiry_days_remaining</th><th>term_expiring_90d_flag</th><th>churn_risk_signal</th><th>rider_description</th><th>rider_category</th></tr></thead><tbody><tr><td>WAIVER</td><td>QUARTERLY</td><td>00099723-3fdc-4073-b709-216fac7561e8</td><td>LL-POL-51862366</td><td>POL-51862366</td><td>Annuity</td><td>4205685.09</td><td>4157.65</td><td>2012-03-22</td><td>2025-12-04</td><td>Active</td><td>Lori Rogers</td><td>Janet Berry</td><td>QC</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>QC</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>00099723-3fdc-4073-b709-216fac7561e8</td><td>null</td><td>false</td><td>DISTINCT</td><td>16630.60</td><td>UNKNOWN</td><td>A</td><td>A</td><td>5191</td><td>-186</td><td>false</td><td>LOW</td><td>null</td><td>UNKNOWN</td></tr><tr><td>ADB</td><td>QUARTERLY</td><td>000ca889-28d6-41e9-9790-117238fe5b0f</td><td>LL-POL-32530538</td><td>POL-32530538</td><td>Critical Illness</td><td>2493493.22</td><td>3214.51</td><td>2020-01-16</td><td>2029-06-12</td><td>Pending</td><td>Amy Powell</td><td>Cole Mitchell</td><td>ON</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>ON</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>000ca889-28d6-41e9-9790-117238fe5b0f</td><td>null</td><td>false</td><td>DISTINCT</td><td>12858.04</td><td>UNKNOWN</td><td>CI</td><td>P</td><td>2335</td><td>1100</td><td>false</td><td>LOW</td><td>null</td><td>UNKNOWN</td></tr><tr><td>ADB</td><td>QUARTERLY</td><td>001e5197-d47e-48e9-a987-a85c49b377aa</td><td>LL-POL-57232279</td><td>POL-57232279</td><td>Whole Life</td><td>1338692.19</td><td>2641.28</td><td>2019-01-03</td><td>2037-10-22</td><td>Cancelled</td><td>Mr. Taylor Stephens MD</td><td>Elizabeth Jones</td><td>ON</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>ON</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>001e5197-d47e-48e9-a987-a85c49b377aa</td><td>null</td><td>false</td><td>DISTINCT</td><td>10565.12</td><td>WHOLE_LIFE</td><td>WHOLELIFE</td><td>C</td><td>2713</td><td>4154</td><td>false</td><td>LOW</td><td>null</td><td>UNKNOWN</td></tr><tr><td>WAIVER</td><td>QUARTERLY</td><td>0021d0bd-14e9-4093-8980-5bcaa2d402ee</td><td>LL-POL-50817102</td><td>POL-50817102</td><td>RRSP</td><td>156676.37</td><td>190.22</td><td>2018-09-19</td><td>2037-09-25</td><td>Active</td><td>Tonya Clarke</td><td>Mary Garcia</td><td>MB</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>MB</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>0021d0bd-14e9-4093-8980-5bcaa2d402ee</td><td>null</td><td>false</td><td>DISTINCT</td><td>760.88</td><td>UNKNOWN</td><td>RRSP</td><td>A</td><td>2819</td><td>4127</td><td>false</td><td>LOW</td><td>null</td><td>UNKNOWN</td></tr><tr><td>WAIVER</td><td>ANNUAL</td><td>0024d6f9-38ac-4323-aca9-a44921d3f2c2</td><td>LL-POL-15485176</td><td>POL-15485176</td><td>RRSP</td><td>1109245.80</td><td>3015.51</td><td>2018-03-02</td><td>2032-12-25</td><td>Suspended</td><td>Sean Bullock</td><td>Christopher Scott</td><td>NS</td><td>null</td><td>null</td><td>2026-06-08T17:44:56.935Z</td><td>ll_policy_admin</td><td>d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>NS</td><td>null</td><td>null</td><td>null</td><td>2026</td><td>6</td><td>0024d6f9-38ac-4323-aca9-a44921d3f2c2</td><td>null</td><td>false</td><td>DISTINCT</td><td>3015.51</td><td>UNKNOWN</td><td>RRSP</td><td>S</td><td>3020</td><td>2392</td><td>false</td><td>LOW</td><td>null</td><td>UNKNOWN</td></tr></tbody></table></div>"
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
         "WAIVER",
         "QUARTERLY",
         "00099723-3fdc-4073-b709-216fac7561e8",
         "LL-POL-51862366",
         "POL-51862366",
         "Annuity",
         "4205685.09",
         "4157.65",
         "2012-03-22",
         "2025-12-04",
         "Active",
         "Lori Rogers",
         "Janet Berry",
         "QC",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "QC",
         null,
         null,
         null,
         2026,
         6,
         "00099723-3fdc-4073-b709-216fac7561e8",
         null,
         false,
         "DISTINCT",
         "16630.60",
         "UNKNOWN",
         "A",
         "A",
         5191,
         -186,
         false,
         "LOW",
         null,
         "UNKNOWN"
        ],
        [
         "ADB",
         "QUARTERLY",
         "000ca889-28d6-41e9-9790-117238fe5b0f",
         "LL-POL-32530538",
         "POL-32530538",
         "Critical Illness",
         "2493493.22",
         "3214.51",
         "2020-01-16",
         "2029-06-12",
         "Pending",
         "Amy Powell",
         "Cole Mitchell",
         "ON",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "ON",
         null,
         null,
         null,
         2026,
         6,
         "000ca889-28d6-41e9-9790-117238fe5b0f",
         null,
         false,
         "DISTINCT",
         "12858.04",
         "UNKNOWN",
         "CI",
         "P",
         2335,
         1100,
         false,
         "LOW",
         null,
         "UNKNOWN"
        ],
        [
         "ADB",
         "QUARTERLY",
         "001e5197-d47e-48e9-a987-a85c49b377aa",
         "LL-POL-57232279",
         "POL-57232279",
         "Whole Life",
         "1338692.19",
         "2641.28",
         "2019-01-03",
         "2037-10-22",
         "Cancelled",
         "Mr. Taylor Stephens MD",
         "Elizabeth Jones",
         "ON",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "ON",
         null,
         null,
         null,
         2026,
         6,
         "001e5197-d47e-48e9-a987-a85c49b377aa",
         null,
         false,
         "DISTINCT",
         "10565.12",
         "WHOLE_LIFE",
         "WHOLELIFE",
         "C",
         2713,
         4154,
         false,
         "LOW",
         null,
         "UNKNOWN"
        ],
        [
         "WAIVER",
         "QUARTERLY",
         "0021d0bd-14e9-4093-8980-5bcaa2d402ee",
         "LL-POL-50817102",
         "POL-50817102",
         "RRSP",
         "156676.37",
         "190.22",
         "2018-09-19",
         "2037-09-25",
         "Active",
         "Tonya Clarke",
         "Mary Garcia",
         "MB",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "MB",
         null,
         null,
         null,
         2026,
         6,
         "0021d0bd-14e9-4093-8980-5bcaa2d402ee",
         null,
         false,
         "DISTINCT",
         "760.88",
         "UNKNOWN",
         "RRSP",
         "A",
         2819,
         4127,
         false,
         "LOW",
         null,
         "UNKNOWN"
        ],
        [
         "WAIVER",
         "ANNUAL",
         "0024d6f9-38ac-4323-aca9-a44921d3f2c2",
         "LL-POL-15485176",
         "POL-15485176",
         "RRSP",
         "1109245.80",
         "3015.51",
         "2018-03-02",
         "2032-12-25",
         "Suspended",
         "Sean Bullock",
         "Christopher Scott",
         "NS",
         null,
         null,
         "2026-06-08T17:44:56.935Z",
         "ll_policy_admin",
         "d5384673142a0a26ad8198d7d660b8dd3e30295beede3459a87fc08765d6dc0a",
         null,
         "MISSING",
         null,
         false,
         null,
         "NS",
         null,
         null,
         null,
         2026,
         6,
         "0024d6f9-38ac-4323-aca9-a44921d3f2c2",
         null,
         false,
         "DISTINCT",
         "3015.51",
         "UNKNOWN",
         "RRSP",
         "S",
         3020,
         2392,
         false,
         "LOW",
         null,
         "UNKNOWN"
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
         "name": "rider_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "premium_frequency_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "legacy_policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "face_amount",
         "type": "\"decimal(15,2)\""
        },
        {
         "metadata": "{}",
         "name": "premium_amount",
         "type": "\"decimal(12,2)\""
        },
        {
         "metadata": "{}",
         "name": "issue_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "expiry_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "policy_status_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "beneficiary_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "underwriting_class_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "date_of_birth",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_standardized",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "sin_hashed",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_year",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_month",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "master_customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "identity_match_confidence",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "identity_manual_review_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "identity_resolution_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "annualised_premium",
         "type": "\"decimal(12,2)\""
        },
        {
         "metadata": "{}",
         "name": "product_category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code_canonical",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_status_canonical",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "policy_tenure_days",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "term_expiry_days_remaining",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "term_expiring_90d_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "churn_risk_signal",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "rider_description",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "rider_category",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>contract_number</th><th>customer_id</th><th>product_type_code</th><th>fund_code</th><th>units</th><th>nav</th><th>market_value</th><th>purchase_date</th><th>currency</th><th>benchmark</th><th>ytd_return_pct</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>allocation_pct</th></tr></thead><tbody><tr><td>INV-00000367</td><td>a193313c-d3eb-456c-ab7a-f5a8a08aeb6e</td><td>GIC</td><td>FND-7702</td><td>4838.6617</td><td>81.0918</td><td>425675.29</td><td>2013-03-05</td><td>CAD</td><td>TSX</td><td>8.05</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td><td>1.0</td></tr><tr><td>INV-00000718</td><td>b8cd7a1e-404f-4cd7-b001-6d3fa2aa286a</td><td>GIC</td><td>FND-0605</td><td>5890.2356</td><td>146.0189</td><td>220181.37</td><td>2020-11-06</td><td>CAD</td><td>MSCI</td><td>19.59</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td><td>1.0</td></tr><tr><td>INV-00001177</td><td>695c71c6-3f81-4c3a-b309-e03b332516e4</td><td>GIC</td><td>FND-0825</td><td>8228.0653</td><td>124.1425</td><td>783219.01</td><td>2015-05-19</td><td>CAD</td><td>FTSE</td><td>13.77</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td><td>1.0</td></tr><tr><td>INV-00001249</td><td>fe72380c-acfb-45fc-a346-0b171280d07a</td><td>Equity</td><td>FND-5523</td><td>5379.2899</td><td>89.8348</td><td>71629.85</td><td>2014-03-24</td><td>CAD</td><td>S&P500</td><td>-7.97</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td><td>1.0</td></tr><tr><td>INV-00005429</td><td>91e46b68-cecd-4eae-96f9-10e4c5cffb5d</td><td>Money Market</td><td>FND-9627</td><td>7251.7386</td><td>154.4084</td><td>658664.23</td><td>2021-01-22</td><td>CAD</td><td>S&P500</td><td>18.85</td><td>2026-06-08T17:47:32.997Z</td><td>climl_invest</td><td>ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804</td><td>1.0</td></tr></tbody></table></div>"
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
         "INV-00000367",
         "a193313c-d3eb-456c-ab7a-f5a8a08aeb6e",
         "GIC",
         "FND-7702",
         4838.6617,
         81.0918,
         425675.29,
         "2013-03-05",
         "CAD",
         "TSX",
         8.05,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804",
         1.0
        ],
        [
         "INV-00000718",
         "b8cd7a1e-404f-4cd7-b001-6d3fa2aa286a",
         "GIC",
         "FND-0605",
         5890.2356,
         146.0189,
         220181.37,
         "2020-11-06",
         "CAD",
         "MSCI",
         19.59,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804",
         1.0
        ],
        [
         "INV-00001177",
         "695c71c6-3f81-4c3a-b309-e03b332516e4",
         "GIC",
         "FND-0825",
         8228.0653,
         124.1425,
         783219.01,
         "2015-05-19",
         "CAD",
         "FTSE",
         13.77,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804",
         1.0
        ],
        [
         "INV-00001249",
         "fe72380c-acfb-45fc-a346-0b171280d07a",
         "Equity",
         "FND-5523",
         5379.2899,
         89.8348,
         71629.85,
         "2014-03-24",
         "CAD",
         "S&P500",
         -7.97,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804",
         1.0
        ],
        [
         "INV-00005429",
         "91e46b68-cecd-4eae-96f9-10e4c5cffb5d",
         "Money Market",
         "FND-9627",
         7251.7386,
         154.4084,
         658664.23,
         "2021-01-22",
         "CAD",
         "S&P500",
         18.85,
         "2026-06-08T17:47:32.997Z",
         "climl_invest",
         "ad07fa5196b08e14f3795d923eb5077fa8e1b6a15f307cac9859b43004d48804",
         1.0
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
         "name": "contract_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_type_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "fund_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "units",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "nav",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "market_value",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "purchase_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "currency",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "benchmark",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ytd_return_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "allocation_pct",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>member_id</th><th>plan_id</th><th>certificate_number</th><th>employer_name</th><th>first_name</th><th>last_name</th><th>date_of_birth</th><th>province</th><th>effective_date</th><th>termination_date</th><th>coverage_type_codes_enrolled</th><th>_ingested_at</th><th>_source_system</th><th>_batch_id</th><th>email_clean</th><th>email_quality_flag</th><th>phone_standardized</th><th>phone_valid_flag</th><th>phone_clean</th><th>province_clean</th><th>postal_code_clean</th><th>postal_code_valid_flag</th><th>sin_hashed</th><th>coverage_type_code</th></tr></thead><tbody><tr><td>MBR-00003362</td><td>GRP-8103</td><td>CERT-MBR-00003362</td><td>Owens, Kim and Wyatt</td><td>Brett</td><td>Salazar</td><td>1980-08-02</td><td>NB</td><td>2018-03-27</td><td>null</td><td>Couple</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>NB</td><td>null</td><td>null</td><td>null</td><td>Couple</td></tr><tr><td>MBR-00005580</td><td>GRP-1891</td><td>CERT-MBR-00005580</td><td>Norris Inc</td><td>Zachary</td><td>Humphrey</td><td>1971-03-23</td><td>PE</td><td>2019-07-22</td><td>null</td><td>Single,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td><td>Single</td></tr><tr><td>MBR-00005580</td><td>GRP-1891</td><td>CERT-MBR-00005580</td><td>Norris Inc</td><td>Zachary</td><td>Humphrey</td><td>1971-03-23</td><td>PE</td><td>2019-07-22</td><td>null</td><td>Single,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td><td>GRP_DENTAL</td></tr><tr><td>MBR-00005580</td><td>GRP-1891</td><td>CERT-MBR-00005580</td><td>Norris Inc</td><td>Zachary</td><td>Humphrey</td><td>1971-03-23</td><td>PE</td><td>2019-07-22</td><td>null</td><td>Single,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>PE</td><td>null</td><td>null</td><td>null</td><td>GRP_LTD</td></tr><tr><td>MBR-00007270</td><td>GRP-2351</td><td>CERT-MBR-00007270</td><td>Sharp, Leblanc and Murray</td><td>Taylor</td><td>Castillo</td><td>1974-10-17</td><td>SK</td><td>2023-08-22</td><td>2023-09-23</td><td>Family,GRP_DENTAL,GRP_LTD</td><td>2026-06-08T17:46:30.179Z</td><td>group_benefits</td><td>b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714</td><td>null</td><td>MISSING</td><td>null</td><td>false</td><td>null</td><td>SK</td><td>null</td><td>null</td><td>null</td><td>Family</td></tr></tbody></table></div>"
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
         "MBR-00003362",
         "GRP-8103",
         "CERT-MBR-00003362",
         "Owens, Kim and Wyatt",
         "Brett",
         "Salazar",
         "1980-08-02",
         "NB",
         "2018-03-27",
         null,
         "Couple",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "NB",
         null,
         null,
         null,
         "Couple"
        ],
        [
         "MBR-00005580",
         "GRP-1891",
         "CERT-MBR-00005580",
         "Norris Inc",
         "Zachary",
         "Humphrey",
         "1971-03-23",
         "PE",
         "2019-07-22",
         null,
         "Single,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null,
         "Single"
        ],
        [
         "MBR-00005580",
         "GRP-1891",
         "CERT-MBR-00005580",
         "Norris Inc",
         "Zachary",
         "Humphrey",
         "1971-03-23",
         "PE",
         "2019-07-22",
         null,
         "Single,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null,
         "GRP_DENTAL"
        ],
        [
         "MBR-00005580",
         "GRP-1891",
         "CERT-MBR-00005580",
         "Norris Inc",
         "Zachary",
         "Humphrey",
         "1971-03-23",
         "PE",
         "2019-07-22",
         null,
         "Single,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "PE",
         null,
         null,
         null,
         "GRP_LTD"
        ],
        [
         "MBR-00007270",
         "GRP-2351",
         "CERT-MBR-00007270",
         "Sharp, Leblanc and Murray",
         "Taylor",
         "Castillo",
         "1974-10-17",
         "SK",
         "2023-08-22",
         "2023-09-23",
         "Family,GRP_DENTAL,GRP_LTD",
         "2026-06-08T17:46:30.179Z",
         "group_benefits",
         "b8eb2aeebd40f072acb0d9b95b1c1f1c064d4784bd4ce05afef31a2ac84b5714",
         null,
         "MISSING",
         null,
         false,
         null,
         "SK",
         null,
         null,
         null,
         "Family"
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
         "name": "member_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "plan_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "certificate_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "employer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "first_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "last_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "date_of_birth",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "province",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "effective_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "termination_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "coverage_type_codes_enrolled",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_quality_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_standardized",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "phone_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "phone_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "province_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_clean",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "postal_code_valid_flag",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "sin_hashed",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "coverage_type_code",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>source_name</th><th>details</th><th>drift_type</th><th>detected_at</th><th>run_id</th></tr></thead><tbody><tr><td>salesforce.crm</td><td>['address', 'advisor_id', 'channel', 'city', 'created_date', 'dob', 'first_name', 'gender', 'last_name', 'source_file_path', 'updated_date']</td><td>UNEXPECTED_COLUMNS</td><td>2026-06-09T15:16:47.499Z</td><td>583cf1e6-0424-4ca7-b920-050732524599</td></tr><tr><td>ll_policy.individual_life</td><td>['beneficiary', 'expiry_date', 'source_file_path', 'underwriter']</td><td>UNEXPECTED_COLUMNS</td><td>2026-06-09T15:16:47.499Z</td><td>583cf1e6-0424-4ca7-b920-050732524599</td></tr><tr><td>gwl_policy.individual_life</td><td>['converted', 'division', 'source_file_path']</td><td>UNEXPECTED_COLUMNS</td><td>2026-06-09T15:16:47.499Z</td><td>583cf1e6-0424-4ca7-b920-050732524599</td></tr><tr><td>sap_billing.invoices</td><td>['currency', 'due_date', 'late_fee', 'payment_date', 'payment_method', 'source_file_path']</td><td>UNEXPECTED_COLUMNS</td><td>2026-06-09T15:16:47.499Z</td><td>583cf1e6-0424-4ca7-b920-050732524599</td></tr><tr><td>climl.seg_fund_contracts</td><td>['benchmark', 'currency', 'nav', 'source_file_path', 'units', 'ytd_return_pct']</td><td>UNEXPECTED_COLUMNS</td><td>2026-06-09T15:16:47.499Z</td><td>583cf1e6-0424-4ca7-b920-050732524599</td></tr></tbody></table></div>"
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
         "salesforce.crm",
         "['address', 'advisor_id', 'channel', 'city', 'created_date', 'dob', 'first_name', 'gender', 'last_name', 'source_file_path', 'updated_date']",
         "UNEXPECTED_COLUMNS",
         "2026-06-09T15:16:47.499Z",
         "583cf1e6-0424-4ca7-b920-050732524599"
        ],
        [
         "ll_policy.individual_life",
         "['beneficiary', 'expiry_date', 'source_file_path', 'underwriter']",
         "UNEXPECTED_COLUMNS",
         "2026-06-09T15:16:47.499Z",
         "583cf1e6-0424-4ca7-b920-050732524599"
        ],
        [
         "gwl_policy.individual_life",
         "['converted', 'division', 'source_file_path']",
         "UNEXPECTED_COLUMNS",
         "2026-06-09T15:16:47.499Z",
         "583cf1e6-0424-4ca7-b920-050732524599"
        ],
        [
         "sap_billing.invoices",
         "['currency', 'due_date', 'late_fee', 'payment_date', 'payment_method', 'source_file_path']",
         "UNEXPECTED_COLUMNS",
         "2026-06-09T15:16:47.499Z",
         "583cf1e6-0424-4ca7-b920-050732524599"
        ],
        [
         "climl.seg_fund_contracts",
         "['benchmark', 'currency', 'nav', 'source_file_path', 'units', 'ytd_return_pct']",
         "UNEXPECTED_COLUMNS",
         "2026-06-09T15:16:47.499Z",
         "583cf1e6-0424-4ca7-b920-050732524599"
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
         "name": "source_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "details",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "drift_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "detected_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "run_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>policy_number</th><th>customer_id</th><th>_source_system</th><th>_ingested_at</th><th>_batch_id</th><th>_dedup_rank</th><th>run_id</th></tr></thead><tbody></tbody></table></div>"
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
       "data": [],
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
         "name": "policy_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "_batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_dedup_rank",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "run_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>contract_number</th><th>total_allocation_pct</th><th>run_id</th><th>detected_at</th></tr></thead><tbody></tbody></table></div>"
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
       "data": [],
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
         "name": "contract_number",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "total_allocation_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "run_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "detected_at",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>target_name</th><th>kind</th><th>write_mode</th><th>row_count</th><th>target_table</th><th>target_path</th><th>status</th><th>message</th></tr></thead><tbody><tr><td>customer.master</td><td>business</td><td>overwrite</td><td>300000</td><td>dbw_c360_canadalife.silver.customer_master</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/customer/master</td><td>SUCCESS</td><td>Processed 300,000 rows in RUN mode</td></tr><tr><td>policy.individual_life_clean</td><td>business</td><td>scd2</td><td>199894</td><td>dbw_c360_canadalife.silver.policy_individual_life_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/individual_life_clean</td><td>SUCCESS</td><td>Processed 199,894 rows in RUN mode</td></tr><tr><td>policy.disability_ci_clean</td><td>business</td><td>overwrite</td><td>19887</td><td>dbw_c360_canadalife.silver.policy_disability_ci_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/disability_ci_clean</td><td>SUCCESS</td><td>Processed 19,887 rows in RUN mode</td></tr><tr><td>digital.portal_clean</td><td>business</td><td>overwrite</td><td>200000</td><td>dbw_c360_canadalife.silver.digital_portal_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/digital/portal_clean</td><td>SUCCESS</td><td>Processed 200,000 rows in RUN mode</td></tr><tr><td>interactions.callcentre_clean</td><td>business</td><td>overwrite</td><td>100000</td><td>dbw_c360_canadalife.silver.interactions_callcentre_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/interactions/callcentre_clean</td><td>SUCCESS</td><td>Processed 100,000 rows in RUN mode</td></tr><tr><td>group_benefits.plan_clean</td><td>business</td><td>overwrite</td><td>100000</td><td>dbw_c360_canadalife.silver.group_benefits_plan_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_benefits/plan_clean</td><td>SUCCESS</td><td>Processed 100,000 rows in RUN mode</td></tr><tr><td>group_benefits.certificate_clean</td><td>business</td><td>overwrite</td><td>99956</td><td>dbw_c360_canadalife.silver.group_benefits_certificate_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_benefits/certificate_clean</td><td>SUCCESS</td><td>Processed 99,956 rows in RUN mode</td></tr><tr><td>freedom55.advisor_feed_clean</td><td>business</td><td>overwrite</td><td>100000</td><td>dbw_c360_canadalife.silver.freedom55_advisor_feed_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/freedom55/advisor_feed_clean</td><td>SUCCESS</td><td>Processed 100,000 rows in RUN mode</td></tr><tr><td>investments.climl_clean</td><td>business</td><td>overwrite</td><td>100000</td><td>dbw_c360_canadalife.silver.investments_climl_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/investments/climl_clean</td><td>SUCCESS</td><td>Processed 100,000 rows in RUN mode</td></tr><tr><td>group_retirement.member_clean</td><td>business</td><td>overwrite</td><td>99954</td><td>dbw_c360_canadalife.silver.group_retirement_member_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_retirement/member_clean</td><td>SUCCESS</td><td>Processed 99,954 rows in RUN mode</td></tr><tr><td>reinsurance.treaty_clean</td><td>business</td><td>overwrite</td><td>10000</td><td>dbw_c360_canadalife.silver.reinsurance_treaty_clean</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reinsurance/treaty_clean</td><td>SUCCESS</td><td>Processed 10,000 rows in RUN mode</td></tr><tr><td>reference.product_code_mapping</td><td>reference</td><td>overwrite</td><td>25</td><td>dbw_c360_canadalife.silver.reference_product_code_mapping</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reference/product_code_mapping</td><td>SUCCESS</td><td>Processed 25 rows in RUN mode</td></tr><tr><td>reference.status_code_mapping</td><td>reference</td><td>overwrite</td><td>17</td><td>dbw_c360_canadalife.silver.reference_status_code_mapping</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reference/status_code_mapping</td><td>SUCCESS</td><td>Processed 17 rows in RUN mode</td></tr><tr><td>reference.rider_codes</td><td>reference</td><td>overwrite</td><td>2</td><td>dbw_c360_canadalife.silver.reference_rider_codes</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reference/rider_codes</td><td>SUCCESS</td><td>Processed 2 rows in RUN mode</td></tr><tr><td>policy.individual_life_enriched</td><td>business</td><td>overwrite</td><td>199894</td><td>dbw_c360_canadalife.silver.policy_individual_life_enriched</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/individual_life_enriched</td><td>SUCCESS</td><td>Processed 199,894 rows in RUN mode</td></tr><tr><td>policy.policy_rider_detail</td><td>business</td><td>overwrite</td><td>99669</td><td>dbw_c360_canadalife.silver.policy_policy_rider_detail</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/policy_rider_detail</td><td>SUCCESS</td><td>Processed 99,669 rows in RUN mode</td></tr><tr><td>investments.fund_allocation_detail</td><td>business</td><td>overwrite</td><td>100000</td><td>dbw_c360_canadalife.silver.investments_fund_allocation_detail</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/investments/fund_allocation_detail</td><td>SUCCESS</td><td>Processed 100,000 rows in RUN mode</td></tr><tr><td>group_benefits.certificate_coverage_detail</td><td>business</td><td>overwrite</td><td>250021</td><td>dbw_c360_canadalife.silver.group_benefits_certificate_coverage_detail</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_benefits/certificate_coverage_detail</td><td>SUCCESS</td><td>Processed 250,021 rows in RUN mode</td></tr><tr><td>monitoring.schema_drift_log</td><td>monitoring</td><td>append</td><td>13</td><td>dbw_c360_canadalife.silver.monitoring_schema_drift_log</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/monitoring/schema_drift_log</td><td>SUCCESS</td><td>Processed 13 rows in RUN mode</td></tr><tr><td>monitoring.dedup_audit_log</td><td>monitoring</td><td>append</td><td>0</td><td>dbw_c360_canadalife.silver.monitoring_dedup_audit_log</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/monitoring/dedup_audit_log</td><td>SUCCESS</td><td>Processed 0 rows in RUN mode</td></tr><tr><td>monitoring.allocation_errors</td><td>monitoring</td><td>append</td><td>0</td><td>dbw_c360_canadalife.silver.monitoring_allocation_errors</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/monitoring/allocation_errors</td><td>SUCCESS</td><td>Processed 0 rows in RUN mode</td></tr></tbody></table></div>"
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
         "customer.master",
         "business",
         "overwrite",
         300000,
         "dbw_c360_canadalife.silver.customer_master",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/customer/master",
         "SUCCESS",
         "Processed 300,000 rows in RUN mode"
        ],
        [
         "policy.individual_life_clean",
         "business",
         "scd2",
         199894,
         "dbw_c360_canadalife.silver.policy_individual_life_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/individual_life_clean",
         "SUCCESS",
         "Processed 199,894 rows in RUN mode"
        ],
        [
         "policy.disability_ci_clean",
         "business",
         "overwrite",
         19887,
         "dbw_c360_canadalife.silver.policy_disability_ci_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/disability_ci_clean",
         "SUCCESS",
         "Processed 19,887 rows in RUN mode"
        ],
        [
         "digital.portal_clean",
         "business",
         "overwrite",
         200000,
         "dbw_c360_canadalife.silver.digital_portal_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/digital/portal_clean",
         "SUCCESS",
         "Processed 200,000 rows in RUN mode"
        ],
        [
         "interactions.callcentre_clean",
         "business",
         "overwrite",
         100000,
         "dbw_c360_canadalife.silver.interactions_callcentre_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/interactions/callcentre_clean",
         "SUCCESS",
         "Processed 100,000 rows in RUN mode"
        ],
        [
         "group_benefits.plan_clean",
         "business",
         "overwrite",
         100000,
         "dbw_c360_canadalife.silver.group_benefits_plan_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_benefits/plan_clean",
         "SUCCESS",
         "Processed 100,000 rows in RUN mode"
        ],
        [
         "group_benefits.certificate_clean",
         "business",
         "overwrite",
         99956,
         "dbw_c360_canadalife.silver.group_benefits_certificate_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_benefits/certificate_clean",
         "SUCCESS",
         "Processed 99,956 rows in RUN mode"
        ],
        [
         "freedom55.advisor_feed_clean",
         "business",
         "overwrite",
         100000,
         "dbw_c360_canadalife.silver.freedom55_advisor_feed_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/freedom55/advisor_feed_clean",
         "SUCCESS",
         "Processed 100,000 rows in RUN mode"
        ],
        [
         "investments.climl_clean",
         "business",
         "overwrite",
         100000,
         "dbw_c360_canadalife.silver.investments_climl_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/investments/climl_clean",
         "SUCCESS",
         "Processed 100,000 rows in RUN mode"
        ],
        [
         "group_retirement.member_clean",
         "business",
         "overwrite",
         99954,
         "dbw_c360_canadalife.silver.group_retirement_member_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_retirement/member_clean",
         "SUCCESS",
         "Processed 99,954 rows in RUN mode"
        ],
        [
         "reinsurance.treaty_clean",
         "business",
         "overwrite",
         10000,
         "dbw_c360_canadalife.silver.reinsurance_treaty_clean",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reinsurance/treaty_clean",
         "SUCCESS",
         "Processed 10,000 rows in RUN mode"
        ],
        [
         "reference.product_code_mapping",
         "reference",
         "overwrite",
         25,
         "dbw_c360_canadalife.silver.reference_product_code_mapping",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reference/product_code_mapping",
         "SUCCESS",
         "Processed 25 rows in RUN mode"
        ],
        [
         "reference.status_code_mapping",
         "reference",
         "overwrite",
         17,
         "dbw_c360_canadalife.silver.reference_status_code_mapping",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reference/status_code_mapping",
         "SUCCESS",
         "Processed 17 rows in RUN mode"
        ],
        [
         "reference.rider_codes",
         "reference",
         "overwrite",
         2,
         "dbw_c360_canadalife.silver.reference_rider_codes",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/reference/rider_codes",
         "SUCCESS",
         "Processed 2 rows in RUN mode"
        ],
        [
         "policy.individual_life_enriched",
         "business",
         "overwrite",
         199894,
         "dbw_c360_canadalife.silver.policy_individual_life_enriched",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/individual_life_enriched",
         "SUCCESS",
         "Processed 199,894 rows in RUN mode"
        ],
        [
         "policy.policy_rider_detail",
         "business",
         "overwrite",
         99669,
         "dbw_c360_canadalife.silver.policy_policy_rider_detail",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/policy_rider_detail",
         "SUCCESS",
         "Processed 99,669 rows in RUN mode"
        ],
        [
         "investments.fund_allocation_detail",
         "business",
         "overwrite",
         100000,
         "dbw_c360_canadalife.silver.investments_fund_allocation_detail",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/investments/fund_allocation_detail",
         "SUCCESS",
         "Processed 100,000 rows in RUN mode"
        ],
        [
         "group_benefits.certificate_coverage_detail",
         "business",
         "overwrite",
         250021,
         "dbw_c360_canadalife.silver.group_benefits_certificate_coverage_detail",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/group_benefits/certificate_coverage_detail",
         "SUCCESS",
         "Processed 250,021 rows in RUN mode"
        ],
        [
         "monitoring.schema_drift_log",
         "monitoring",
         "append",
         13,
         "dbw_c360_canadalife.silver.monitoring_schema_drift_log",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/monitoring/schema_drift_log",
         "SUCCESS",
         "Processed 13 rows in RUN mode"
        ],
        [
         "monitoring.dedup_audit_log",
         "monitoring",
         "append",
         0,
         "dbw_c360_canadalife.silver.monitoring_dedup_audit_log",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/monitoring/dedup_audit_log",
         "SUCCESS",
         "Processed 0 rows in RUN mode"
        ],
        [
         "monitoring.allocation_errors",
         "monitoring",
         "append",
         0,
         "dbw_c360_canadalife.silver.monitoring_allocation_errors",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/monitoring/allocation_errors",
         "SUCCESS",
         "Processed 0 rows in RUN mode"
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
         "name": "target_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "kind",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "write_mode",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "row_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "target_table",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "target_path",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "message",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "# ------------------------------------------------------------------------------\n",
    "# 5. Target Builders\n",
    "# ------------------------------------------------------------------------------\n",
    "def build_customer_master():\n",
    "    contact_df = normalize_salesforce_for_customer().select(\n",
    "        \"customer_id\", \"first_name\", \"last_name\", \"email_clean\", \"email_quality_flag\", \"phone_clean\", \"province_clean\", \"postal_code_clean\", \"postal_code_valid_flag\", \"channel\", \"advisor_id\", \"_ingested_at\", \"_source_system\", \"_batch_id\"\n",
    "    )\n",
    "    gwl_df = normalize_gwl_policy().select(\n",
    "        \"customer_id\", \"product_type_code\", \"province_clean\", \"postal_code_clean\", \"postal_code_valid_flag\", \"_ingested_at\", \"_source_system\", \"_batch_id\"\n",
    "    )\n",
    "    sap_source_df = read_bronze_source(\"sap_billing.invoices\")\n",
    "    sap_source_columns = get_column_names(sap_source_df)\n",
    "    sap_df = sap_source_df.select(\n",
    "        safe_col(sap_source_columns, \"customer_id\").alias(\"customer_id\"),\n",
    "        safe_col(sap_source_columns, \"policy_id\").alias(\"policy_number\"),\n",
    "        safe_col(sap_source_columns, \"status\").alias(\"billing_status\"),\n",
    "        safe_col(sap_source_columns, \"amount\").cast(T.DecimalType(12, 2)).alias(\"billing_amount\"),\n",
    "        safe_col(sap_source_columns, \"ingestion_timestamp\").cast(\"timestamp\").alias(\"_ingested_at\"),\n",
    "        safe_col(sap_source_columns, \"source_system\").alias(\"_source_system\"),\n",
    "        build_batch_id_expr(sap_source_columns).alias(\"_batch_id\"),\n",
    "    )\n",
    "    combined_df = union_all([contact_df, gwl_df, sap_df])\n",
    "    policy_identity_df = build_target_dataframe(\"policy.individual_life_clean\").select(\"customer_id\", \"master_customer_id\").dropDuplicates([\"customer_id\"])\n",
    "    mastered_df = combined_df.join(F.broadcast(policy_identity_df), [\"customer_id\"], \"left\").withColumn(\n",
    "        \"master_customer_id\", F.coalesce(F.col(\"master_customer_id\"), F.col(\"customer_id\"))\n",
    "    )\n",
    "    master_window = Window.partitionBy(\"master_customer_id\").orderBy(\n",
    "        F.col(\"email_clean\").isNotNull().desc(),\n",
    "        F.col(\"phone_clean\").isNotNull().desc(),\n",
    "        F.col(\"advisor_id\").isNotNull().desc(),\n",
    "        F.col(\"_ingested_at\").desc_nulls_last(),\n",
    "    )\n",
    "    return mastered_df.withColumn(\"_row_num\", F.row_number().over(master_window)).filter(F.col(\"_row_num\") == 1).drop(\"_row_num\").withColumn(\n",
    "        \"source_customer_id\", F.col(\"customer_id\")\n",
    "    ).withColumn(\n",
    "        \"customer_id\", F.col(\"master_customer_id\")\n",
    "    ).drop(\"master_customer_id\")\n",
    "\n",
    "\n",
    "def build_policy_individual_life_clean():\n",
    "    global DEDUP_AUDIT_CACHE\n",
    "\n",
    "    customer_lookup = build_customer_contact_lookup()\n",
    "    ll_df = normalize_ll_policy()\n",
    "    gwl_df = normalize_gwl_policy()\n",
    "    combined_df = union_all([ll_df, gwl_df]).join(customer_lookup, [\"customer_id\"], \"left\")\n",
    "\n",
    "    policy_df = combined_df.withColumns({\n",
    "        \"email_clean\": F.coalesce(F.col(\"email_clean\"), F.col(\"contact_email_clean\")),\n",
    "        \"email_quality_flag\": F.coalesce(F.col(\"email_quality_flag\"), F.col(\"contact_email_quality_flag\")),\n",
    "        \"phone_clean\": F.coalesce(F.col(\"phone_clean\"), F.col(\"contact_phone_clean\")),\n",
    "        \"province_clean\": F.coalesce(F.col(\"province_clean\"), F.col(\"contact_province_clean\")),\n",
    "        \"postal_code_clean\": F.coalesce(F.col(\"postal_code_clean\"), F.col(\"contact_postal_code_clean\")),\n",
    "        \"postal_code_valid_flag\": F.coalesce(F.col(\"postal_code_valid_flag\"), F.col(\"contact_postal_code_valid_flag\")),\n",
    "        \"_ingested_year\": F.year(F.col(\"_ingested_at\")),\n",
    "        \"_ingested_month\": F.month(F.col(\"_ingested_at\")),\n",
    "    }).drop(\n",
    "        \"contact_email_clean\",\n",
    "        \"contact_email_quality_flag\",\n",
    "        \"contact_phone_clean\",\n",
    "        \"contact_province_clean\",\n",
    "        \"contact_postal_code_clean\",\n",
    "        \"contact_postal_code_valid_flag\",\n",
    "    )\n",
    "\n",
    "    intra_window = Window.partitionBy(\"policy_number\").orderBy(F.col(\"_ingested_at\").desc_nulls_last())\n",
    "    intra_ranked = policy_df.withColumn(\"_row_num\", F.row_number().over(intra_window))\n",
    "    intra_dedup_df = intra_ranked.filter(F.col(\"_row_num\") == 1).drop(\"_row_num\")\n",
    "\n",
    "    cross_window = Window.partitionBy(\"customer_id\", \"product_type_code\", \"issue_date\", \"face_amount\").orderBy(\n",
    "        F.when(F.col(\"_source_system\").contains(\"ll\"), 1).when(F.col(\"_source_system\").contains(\"gwl\"), 2).otherwise(99),\n",
    "        F.col(\"_ingested_at\").desc_nulls_last(),\n",
    "    )\n",
    "    cross_ranked = intra_dedup_df.withColumn(\"_dedup_rank\", F.row_number().over(cross_window)).withColumn(\"_is_duplicate\", F.col(\"_dedup_rank\") > 1)\n",
    "\n",
    "    DEDUP_AUDIT_CACHE = cross_ranked.filter(F.col(\"_is_duplicate\") == True).select(\n",
    "        \"policy_number\", \"customer_id\", \"_source_system\", \"_ingested_at\", \"_batch_id\", \"_dedup_rank\"\n",
    "    )\n",
    "\n",
    "    clean_df = cross_ranked.filter(F.col(\"_is_duplicate\") == False).drop(\"_dedup_rank\", \"_is_duplicate\")\n",
    "    auto_merge_df, review_queue_df = persist_identity_resolution_artifacts(clean_df)\n",
    "    review_customer_df = review_queue_df.select(F.col(\"left_customer_id\").alias(\"customer_id\")).unionByName(\n",
    "        review_queue_df.select(F.col(\"right_customer_id\").alias(\"customer_id\")),\n",
    "        allowMissingColumns=True,\n",
    "    ).dropDuplicates([\"customer_id\"])\n",
    "    clean_df = clean_df.join(F.broadcast(auto_merge_df), [\"customer_id\"], \"left\").join(\n",
    "        F.broadcast(review_customer_df.withColumn(\"identity_manual_review_flag\", F.lit(True))),\n",
    "        [\"customer_id\"],\n",
    "        \"left\",\n",
    "    ).withColumns({\n",
    "        \"master_customer_id\": F.coalesce(F.col(\"master_customer_id\"), F.col(\"customer_id\")),\n",
    "        \"identity_match_confidence\": F.col(\"identity_match_confidence\"),\n",
    "        \"identity_manual_review_flag\": F.coalesce(F.col(\"identity_manual_review_flag\"), F.lit(False)),\n",
    "        \"identity_resolution_status\": F.when(F.col(\"identity_match_confidence\").isNotNull(), F.lit(\"AUTO_MERGED\")).when(F.col(\"identity_manual_review_flag\") == True, F.lit(\"PENDING_MANUAL_REVIEW\")).otherwise(F.lit(\"DISTINCT\")),\n",
    "    })\n",
    "    if execution_mode == \"RUN\":\n",
    "        enforce_null_gate(clean_df, [\"policy_number\", \"_ingested_at\", \"_source_system\"], dq_threshold_pct)\n",
    "    return clean_df\n",
    "\n",
    "\n",
    "def build_policy_disability_ci_clean():\n",
    "    ll_df = normalize_ll_policy()\n",
    "    filtered_df = ll_df.filter(F.upper(F.coalesce(F.col(\"product_type_code\"), F.lit(\"\"))).rlike(\"(DI|DISABILITY|CI|CRITICAL)\"))\n",
    "    return deduplicate_by_window(filtered_df, [\"policy_number\"], [\"_ingested_at\"])\n",
    "\n",
    "\n",
    "def canonicalise_product_expr(column_expr):\n",
    "    normalized = F.upper(F.regexp_replace(F.coalesce(column_expr, F.lit(\"UNKNOWN\")), r\"[^A-Z0-9]\", \"\"))\n",
    "    return (\n",
    "        F.when(normalized.isin(\"T20\", \"TERM20\", \"LT20\"), F.lit(\"TERM20\"))\n",
    "        .when(normalized.isin(\"WL\", \"WHOLELIFE\"), F.lit(\"WHOLELIFE\"))\n",
    "        .when(normalized.isin(\"UL\", \"UNIVERSALLIFE\"), F.lit(\"UNIVERSALLIFE\"))\n",
    "        .otherwise(normalized)\n",
    "    )\n",
    "\n",
    "\n",
    "def canonicalise_status_expr(column_expr):\n",
    "    normalized = F.upper(F.regexp_replace(F.coalesce(column_expr, F.lit(\"UNKNOWN\")), r\"[^A-Z0-9]\", \"\"))\n",
    "    return (\n",
    "        F.when(normalized.isin(\"01\", \"ACT\", \"ACTIVE\", \"INFORCE\"), F.lit(\"INFORCE\"))\n",
    "        .when(normalized.isin(\"03\", \"LAP\", \"LAPSE\", \"LAPSED\"), F.lit(\"LAPSED\"))\n",
    "        .when(normalized.isin(\"GRACE\", \"GRACEPERIOD\"), F.lit(\"GRACE\"))\n",
    "        .when(normalized.isin(\"PAIDUP\"), F.lit(\"PAID_UP\"))\n",
    "        .when(normalized.isin(\"SUSPENDED\"), F.lit(\"SUSPENDED\"))\n",
    "        .when(normalized.isin(\"PENDING\"), F.lit(\"PENDING\"))\n",
    "        .when(normalized.isin(\"CANCELLED\", \"CANCELED\"), F.lit(\"CANCELLED\"))\n",
    "        .otherwise(normalized)\n",
    "    )\n",
    "\n",
    "\n",
    "def build_reference_product_code_mapping():\n",
    "    policy_df = build_target_dataframe(\"policy.individual_life_clean\")\n",
    "    investments_df = build_target_dataframe(\"investments.climl_clean\")\n",
    "    advisor_df = build_target_dataframe(\"freedom55.advisor_feed_clean\")\n",
    "\n",
    "    mapping_df = union_all([\n",
    "        policy_df.select(F.col(\"product_type_code\").alias(\"legacy_code\"), F.col(\"_source_system\").alias(\"source_system\")),\n",
    "        investments_df.select(F.col(\"product_type_code\").alias(\"legacy_code\"), F.col(\"_source_system\").alias(\"source_system\")),\n",
    "        advisor_df.select(F.col(\"product_type_code\").alias(\"legacy_code\"), F.col(\"_source_system\").alias(\"source_system\")),\n",
    "    ]).filter(F.col(\"legacy_code\").isNotNull()).dropDuplicates([\"legacy_code\", \"source_system\"])\n",
    "\n",
    "    return mapping_df.withColumn(\"canonical_code\", canonicalise_product_expr(F.col(\"legacy_code\"))).withColumn(\n",
    "        \"product_category\",\n",
    "        F.when(F.col(\"canonical_code\").rlike(\"TERM\"), F.lit(\"TERM_LIFE\")).when(F.col(\"canonical_code\").rlike(\"WHOLE\"), F.lit(\"WHOLE_LIFE\")).otherwise(F.lit(\"UNKNOWN\")),\n",
    "    )\n",
    "\n",
    "\n",
    "def build_reference_status_code_mapping():\n",
    "    policy_df = build_target_dataframe(\"policy.individual_life_clean\")\n",
    "    reinsurance_df = build_target_dataframe(\"reinsurance.treaty_clean\")\n",
    "    interactions_df = build_target_dataframe(\"interactions.callcentre_clean\")\n",
    "\n",
    "    mapping_df = union_all([\n",
    "        policy_df.select(F.col(\"policy_status_code\").alias(\"legacy_code\"), F.col(\"_source_system\").alias(\"source_system\")),\n",
    "        reinsurance_df.select(F.col(\"policy_status_code\").alias(\"legacy_code\"), F.col(\"_source_system\").alias(\"source_system\")),\n",
    "        interactions_df.select(F.col(\"interaction_status\").alias(\"legacy_code\"), F.col(\"_source_system\").alias(\"source_system\")),\n",
    "    ]).filter(F.col(\"legacy_code\").isNotNull()).dropDuplicates([\"legacy_code\", \"source_system\"])\n",
    "\n",
    "    return mapping_df.withColumn(\"canonical_status_code\", canonicalise_status_expr(F.col(\"legacy_code\")))\n",
    "\n",
    "\n",
    "def build_reference_rider_codes():\n",
    "    policy_df = build_target_dataframe(\"policy.individual_life_clean\")\n",
    "    rider_df = policy_df.withColumn(\"rider_code\", F.explode_outer(F.split(F.coalesce(F.col(\"rider_codes\"), F.lit(\"\")), \",\"))).withColumn(\"rider_code\", F.trim(F.col(\"rider_code\"))).filter(F.col(\"rider_code\") != \"\")\n",
    "    return rider_df.select(\"rider_code\").dropDuplicates().withColumn(\"rider_description\", F.lit(None).cast(\"string\")).withColumn(\"rider_category\", F.lit(\"UNKNOWN\"))\n",
    "\n",
    "\n",
    "def build_policy_individual_life_enriched():\n",
    "    policy_df = build_target_dataframe(\"policy.individual_life_clean\")\n",
    "    product_map_df = build_target_dataframe(\"reference.product_code_mapping\")\n",
    "    status_map_df = build_target_dataframe(\"reference.status_code_mapping\")\n",
    "\n",
    "    enriched_df = policy_df.join(F.broadcast(FREQ_MAP_DF), [\"premium_frequency_code\"], \"left\").withColumn(\n",
    "        \"annualised_premium\",\n",
    "        F.round(F.col(\"premium_amount\") * F.coalesce(F.col(\"freq_multiplier\"), F.lit(1)), 2).cast(T.DecimalType(12, 2)),\n",
    "    ).drop(\"freq_multiplier\")\n",
    "\n",
    "    product_map_lookup_df = F.broadcast(product_map_df.select(\n",
    "        F.col(\"legacy_code\").alias(\"product_legacy_code\"),\n",
    "        F.col(\"source_system\").alias(\"product_source_system\"),\n",
    "        F.col(\"canonical_code\"),\n",
    "        F.col(\"product_category\"),\n",
    "    ))\n",
    "    enriched_df = enriched_df.join(\n",
    "        product_map_lookup_df,\n",
    "        (enriched_df[\"product_type_code\"] == product_map_lookup_df[\"product_legacy_code\"]) &\n",
    "        (enriched_df[\"_source_system\"] == product_map_lookup_df[\"product_source_system\"]),\n",
    "        \"left\",\n",
    "    ).withColumns({\n",
    "        \"product_type_code_canonical\": F.coalesce(F.col(\"canonical_code\"), enriched_df[\"product_type_code\"]),\n",
    "        \"product_category\": F.coalesce(F.col(\"product_category\"), F.lit(\"UNKNOWN\")),\n",
    "    }).drop(\"product_legacy_code\", \"product_source_system\", \"canonical_code\")\n",
    "\n",
    "    status_map_lookup_df = F.broadcast(status_map_df.select(\n",
    "        F.col(\"legacy_code\").alias(\"status_legacy_code\"),\n",
    "        F.col(\"source_system\").alias(\"status_source_system\"),\n",
    "        F.col(\"canonical_status_code\"),\n",
    "    ))\n",
    "    enriched_df = enriched_df.join(\n",
    "        status_map_lookup_df,\n",
    "        (enriched_df[\"policy_status_code\"] == status_map_lookup_df[\"status_legacy_code\"]) &\n",
    "        (enriched_df[\"_source_system\"] == status_map_lookup_df[\"status_source_system\"]),\n",
    "        \"left\",\n",
    "    ).withColumn(\n",
    "        \"policy_status_canonical\", F.coalesce(F.col(\"canonical_status_code\"), enriched_df[\"policy_status_code\"])\n",
    "    ).drop(\"status_legacy_code\", \"status_source_system\", \"canonical_status_code\")\n",
    "\n",
    "    reference_date = F.to_date(F.lit(run_date)) if run_date else F.current_date()\n",
    "    enriched_df = enriched_df.withColumn(\"policy_tenure_days\", F.datediff(reference_date, F.col(\"issue_date\"))).withColumn(\n",
    "        \"term_expiry_days_remaining\",\n",
    "        F.when(F.col(\"expiry_date\").isNotNull(), F.datediff(F.col(\"expiry_date\"), reference_date)),\n",
    "    ).withColumn(\n",
    "        \"term_expiring_90d_flag\", F.when(F.col(\"term_expiry_days_remaining\").between(0, 90), F.lit(True)).otherwise(F.lit(False))\n",
    "    ).withColumn(\n",
    "        \"churn_risk_signal\",\n",
    "        F.when(F.col(\"policy_status_canonical\") == \"GRACE\", F.lit(\"HIGH\")).when(F.col(\"term_expiring_90d_flag\") == True, F.lit(\"HIGH\")).when(F.col(\"policy_tenure_days\") < 365, F.lit(\"MEDIUM\")).otherwise(F.lit(\"LOW\")),\n",
    "    )\n",
    "\n",
    "    return enriched_df\n",
    "\n",
    "\n",
    "def build_policy_rider_detail():\n",
    "    enriched_df = build_target_dataframe(\"policy.individual_life_enriched\")\n",
    "    rider_ref_df = build_target_dataframe(\"reference.rider_codes\")\n",
    "    rider_df = enriched_df.withColumn(\"rider_code\", F.explode_outer(F.split(F.coalesce(F.col(\"rider_codes\"), F.lit(\"\")), \",\"))).withColumn(\"rider_code\", F.trim(F.col(\"rider_code\"))).filter(F.col(\"rider_code\") != \"\").drop(\"rider_codes\")\n",
    "    return rider_df.join(F.broadcast(rider_ref_df), [\"rider_code\"], \"left\")\n",
    "\n",
    "\n",
    "def build_digital_portal_clean():\n",
    "    adobe_df = read_adobe_json_events()\n",
    "    portal_df = normalize_portal_events()\n",
    "    combined_df = union_all([adobe_df, portal_df])\n",
    "    return deduplicate_by_window(combined_df, [\"event_id\"], [\"event_timestamp\", \"_ingested_at\"])\n",
    "\n",
    "\n",
    "def build_group_benefits_plan_clean():\n",
    "    base_df = normalize_group_benefits_base()\n",
    "    return deduplicate_by_window(base_df, [\"plan_id\", \"member_id\"], [\"_ingested_at\"])\n",
    "\n",
    "\n",
    "def build_group_benefits_certificate_clean():\n",
    "    base_df = normalize_group_benefits_base()\n",
    "    return deduplicate_by_window(base_df, [\"certificate_number\"], [\"_ingested_at\"])\n",
    "\n",
    "\n",
    "def build_group_benefits_certificate_coverage_detail():\n",
    "    certificate_df = build_target_dataframe(\"group_benefits.certificate_clean\")\n",
    "    return certificate_df.withColumn(\"coverage_type_code\", F.explode_outer(F.split(F.coalesce(F.col(\"coverage_type_codes_enrolled\"), F.lit(\"\")), \",\"))).withColumn(\"coverage_type_code\", F.trim(F.col(\"coverage_type_code\"))).filter(F.col(\"coverage_type_code\") != \"\")\n",
    "\n",
    "\n",
    "def build_freedom55_advisor_feed_clean():\n",
    "    return deduplicate_by_window(normalize_freedom55_assignments(), [\"advisor_id\", \"assignment_id\"], [\"_ingested_at\"])\n",
    "\n",
    "\n",
    "def build_investments_climl_clean():\n",
    "    return deduplicate_by_window(normalize_climl_contracts(), [\"contract_number\", \"fund_code\"], [\"_ingested_at\"])\n",
    "\n",
    "\n",
    "def build_investments_fund_allocation_detail():\n",
    "    global ALLOCATION_ERROR_CACHE\n",
    "\n",
    "    investments_df = build_target_dataframe(\"investments.climl_clean\")\n",
    "    if \"fund_code\" in investments_df.columns and \"market_value\" in investments_df.columns:\n",
    "        window_spec = Window.partitionBy(\"contract_number\")\n",
    "        detail_df = investments_df.withColumn(\n",
    "            \"allocation_pct\",\n",
    "            F.when(F.sum(F.col(\"market_value\")).over(window_spec) != 0, F.col(\"market_value\") / F.sum(F.col(\"market_value\")).over(window_spec)).otherwise(F.lit(None).cast(\"double\")),\n",
    "        )\n",
    "    else:\n",
    "        detail_df = investments_df.withColumn(\"allocation_pct\", F.lit(None).cast(\"double\"))\n",
    "\n",
    "    alloc_check_df = detail_df.groupBy(\"contract_number\").agg(F.round(F.sum(F.col(\"allocation_pct\")), 4).alias(\"total_allocation_pct\")).filter(F.abs(F.col(\"total_allocation_pct\") - 1.0) > 0.001)\n",
    "    ALLOCATION_ERROR_CACHE = alloc_check_df.withColumn(\"run_id\", F.lit(run_id)).withColumn(\"detected_at\", F.current_timestamp())\n",
    "    return detail_df\n",
    "\n",
    "\n",
    "def build_group_retirement_member_clean():\n",
    "    return deduplicate_by_window(normalize_group_retirement_members(), [\"member_id\"], [\"_ingested_at\"])\n",
    "\n",
    "\n",
    "def build_reinsurance_treaty_clean():\n",
    "    return deduplicate_by_window(normalize_reinsurance_treaties(), [\"treaty_id\"], [\"_ingested_at\"])\n",
    "\n",
    "\n",
    "def build_schema_drift_log():\n",
    "    drift_rows = []\n",
    "    for source_name_value in BRONZE_TABLES:\n",
    "        current_columns, unexpected_columns, missing_columns = schema_signature(source_name_value)\n",
    "        if unexpected_columns:\n",
    "            drift_rows.append((source_name_value, str(unexpected_columns), \"UNEXPECTED_COLUMNS\"))\n",
    "        if missing_columns:\n",
    "            drift_rows.append((source_name_value, str(missing_columns), \"MISSING_EXPECTED_COLUMNS\"))\n",
    "        if source_name_value == \"adobe_analytics.digital_events\" and \"[\" in current_columns:\n",
    "            drift_rows.append((source_name_value, \"Adobe bronze contains raw JSON lines in a single '[' column; reparsing raw JSON file path in silver.\", \"RAW_JSON_LINE_SPLIT\"))\n",
    "\n",
    "    drift_schema = T.StructType([\n",
    "        T.StructField(\"source_name\", T.StringType(), True),\n",
    "        T.StructField(\"details\", T.StringType(), True),\n",
    "        T.StructField(\"drift_type\", T.StringType(), True),\n",
    "    ])\n",
    "    drift_df = spark.createDataFrame(drift_rows, drift_schema) if drift_rows else spark.createDataFrame([], drift_schema)\n",
    "    return drift_df.withColumn(\"detected_at\", F.current_timestamp()).withColumn(\"run_id\", F.lit(run_id))\n",
    "\n",
    "\n",
    "def build_dedup_audit_log():\n",
    "    if DEDUP_AUDIT_CACHE is None:\n",
    "        build_target_dataframe(\"policy.individual_life_clean\")\n",
    "    if DEDUP_AUDIT_CACHE is None:\n",
    "        schema = T.StructType([\n",
    "            T.StructField(\"policy_number\", T.StringType(), True),\n",
    "            T.StructField(\"customer_id\", T.StringType(), True),\n",
    "            T.StructField(\"_source_system\", T.StringType(), True),\n",
    "            T.StructField(\"_ingested_at\", T.TimestampType(), True),\n",
    "            T.StructField(\"_batch_id\", T.StringType(), True),\n",
    "            T.StructField(\"_dedup_rank\", T.IntegerType(), True),\n",
    "        ])\n",
    "        empty_df = spark.createDataFrame([], schema)\n",
    "        return empty_df.withColumn(\"run_id\", F.lit(run_id))\n",
    "    return DEDUP_AUDIT_CACHE.withColumn(\"run_id\", F.lit(run_id))\n",
    "\n",
    "\n",
    "def build_allocation_errors():\n",
    "    if ALLOCATION_ERROR_CACHE is None:\n",
    "        build_target_dataframe(\"investments.fund_allocation_detail\")\n",
    "    if ALLOCATION_ERROR_CACHE is None:\n",
    "        schema = T.StructType([\n",
    "            T.StructField(\"contract_number\", T.StringType(), True),\n",
    "            T.StructField(\"total_allocation_pct\", T.DoubleType(), True),\n",
    "            T.StructField(\"run_id\", T.StringType(), True),\n",
    "            T.StructField(\"detected_at\", T.TimestampType(), True),\n",
    "        ])\n",
    "        return spark.createDataFrame([], schema)\n",
    "    return ALLOCATION_ERROR_CACHE\n",
    "\n",
    "\n",
    "TARGET_BUILDERS = {\n",
    "    \"customer.master\": build_customer_master,\n",
    "    \"policy.individual_life_clean\": build_policy_individual_life_clean,\n",
    "    \"policy.disability_ci_clean\": build_policy_disability_ci_clean,\n",
    "    \"reference.product_code_mapping\": build_reference_product_code_mapping,\n",
    "    \"reference.status_code_mapping\": build_reference_status_code_mapping,\n",
    "    \"reference.rider_codes\": build_reference_rider_codes,\n",
    "    \"policy.individual_life_enriched\": build_policy_individual_life_enriched,\n",
    "    \"policy.policy_rider_detail\": build_policy_rider_detail,\n",
    "    \"digital.portal_clean\": build_digital_portal_clean,\n",
    "    \"interactions.callcentre_clean\": normalize_callcentre_interactions,\n",
    "    \"group_benefits.plan_clean\": build_group_benefits_plan_clean,\n",
    "    \"group_benefits.certificate_clean\": build_group_benefits_certificate_clean,\n",
    "    \"group_benefits.certificate_coverage_detail\": build_group_benefits_certificate_coverage_detail,\n",
    "    \"freedom55.advisor_feed_clean\": build_freedom55_advisor_feed_clean,\n",
    "    \"investments.climl_clean\": build_investments_climl_clean,\n",
    "    \"investments.fund_allocation_detail\": build_investments_fund_allocation_detail,\n",
    "    \"group_retirement.member_clean\": build_group_retirement_member_clean,\n",
    "    \"reinsurance.treaty_clean\": build_reinsurance_treaty_clean,\n",
    "    \"monitoring.schema_drift_log\": build_schema_drift_log,\n",
    "    \"monitoring.dedup_audit_log\": build_dedup_audit_log,\n",
    "    \"monitoring.allocation_errors\": build_allocation_errors,\n",
    "}\n",
    "\n",
    "\n",
    "def build_target_dataframe(target_name_value: str):\n",
    "    if target_name_value in DATAFRAME_CACHE:\n",
    "        return DATAFRAME_CACHE[target_name_value]\n",
    "\n",
    "    builder = TARGET_BUILDERS.get(target_name_value)\n",
    "    if builder is None:\n",
    "        raise ValueError(f\"Unsupported target: {target_name_value}\")\n",
    "\n",
    "    df = builder()\n",
    "    DATAFRAME_CACHE[target_name_value] = df\n",
    "    return df\n",
    "\n",
    "# ------------------------------------------------------------------------------\n",
    "# 6. Execution\n",
    "# ------------------------------------------------------------------------------\n",
    "selected_targets = resolve_selected_targets()\n",
    "ensure_silver_schema()\n",
    "result_rows = []\n",
    "\n",
    "if execution_mode == \"PLAN\":\n",
    "    for target_name_value in selected_targets:\n",
    "        target_config = TARGET_CONFIG[target_name_value]\n",
    "        source_details = []\n",
    "        adobe_json_flag = False\n",
    "        for source_value in target_config[\"sources\"]:\n",
    "            if source_value in BRONZE_TABLES:\n",
    "                source_fqn = bronze_table_fqn(source_value)\n",
    "                exists_flag = table_exists(source_fqn)\n",
    "                source_details.append(f\"{source_value} -> {source_fqn} (exists={exists_flag})\")\n",
    "                if source_value == \"adobe_analytics.digital_events\":\n",
    "                    adobe_json_flag = True\n",
    "            else:\n",
    "                source_details.append(f\"{source_value} -> derived silver dependency\")\n",
    "        result_rows.append({\n",
    "            \"target_name\": target_name_value,\n",
    "            \"kind\": target_config[\"kind\"],\n",
    "            \"write_mode\": target_config[\"write_mode\"],\n",
    "            \"sources\": \" | \".join(source_details),\n",
    "            \"json_flattening\": \"ADOBE_ONLY\" if adobe_json_flag else \"NO_JSON_FLATTENING\",\n",
    "            \"target_table\": silver_table_fqn(target_name_value),\n",
    "            \"target_path\": silver_storage_path(target_name_value),\n",
    "            \"status\": \"READY\",\n",
    "        })\n",
    "    display(spark.createDataFrame(result_rows))\n",
    "else:\n",
    "    for target_name_value in selected_targets:\n",
    "        target_config = TARGET_CONFIG[target_name_value]\n",
    "        try:\n",
    "            target_df = build_target_dataframe(target_name_value)\n",
    "            target_count = target_df.count()\n",
    "            try:\n",
    "                preview_rows = target_df.limit(5).collect()\n",
    "                preview_df = spark.createDataFrame(preview_rows, target_df.schema) if preview_rows else spark.createDataFrame([], target_df.schema)\n",
    "                display(preview_df)\n",
    "            except Exception as preview_exc:\n",
    "                print(f\"PREVIEW_SKIPPED|{target_name_value}|{type(preview_exc).__name__}: {str(preview_exc)}\")\n",
    "\n",
    "            if execution_mode == \"RUN\":\n",
    "                if target_config[\"write_mode\"] == \"scd2\":\n",
    "                    target_df = apply_scd2(target_df, target_name_value, target_config[\"keys\"][0])\n",
    "                else:\n",
    "                    write_delta(target_df, target_name_value, target_config[\"write_mode\"])\n",
    "                maybe_optimize(target_name_value, target_config[\"keys\"])\n",
    "\n",
    "            result_rows.append({\n",
    "                \"target_name\": target_name_value,\n",
    "                \"kind\": target_config[\"kind\"],\n",
    "                \"write_mode\": target_config[\"write_mode\"],\n",
    "                \"row_count\": target_count,\n",
    "                \"target_table\": silver_table_fqn(target_name_value),\n",
    "                \"target_path\": silver_storage_path(target_name_value),\n",
    "                \"status\": \"SUCCESS\",\n",
    "                \"message\": f\"Processed {target_count:,} rows in {execution_mode} mode\",\n",
    "            })\n",
    "        except Exception as exc:\n",
    "            error_message = f\"{type(exc).__name__}: {str(exc)}\"\n",
    "            print(f\"TARGET_FAILED|{target_name_value}|{error_message}\")\n",
    "            result_rows.append({\n",
    "                \"target_name\": target_name_value,\n",
    "                \"kind\": target_config[\"kind\"],\n",
    "                \"write_mode\": target_config[\"write_mode\"],\n",
    "                \"row_count\": None,\n",
    "                \"target_table\": silver_table_fqn(target_name_value),\n",
    "                \"target_path\": silver_storage_path(target_name_value),\n",
    "                \"status\": \"FAILED\",\n",
    "                \"message\": error_message,\n",
    "            })\n",
    "\n",
    "    result_schema = T.StructType([\n",
    "        T.StructField(\"target_name\", T.StringType(), True),\n",
    "        T.StructField(\"kind\", T.StringType(), True),\n",
    "        T.StructField(\"write_mode\", T.StringType(), True),\n",
    "        T.StructField(\"row_count\", T.LongType(), True),\n",
    "        T.StructField(\"target_table\", T.StringType(), True),\n",
    "        T.StructField(\"target_path\", T.StringType(), True),\n",
    "        T.StructField(\"status\", T.StringType(), True),\n",
    "        T.StructField(\"message\", T.StringType(), True),\n",
    "    ])\n",
    "    result_df = spark.createDataFrame(result_rows, result_schema)\n",
    "    display(result_df)\n",
    "\n",
    "    failed_rows = [row for row in result_rows if row.get(\"status\") == \"FAILED\"]\n",
    "    if failed_rows:\n",
    "        print(\"FAILED_TARGET_SUMMARY_START\")\n",
    "        for failed_row in failed_rows:\n",
    "            print(\n",
    "                f\"FAILED_TARGET|{failed_row['target_name']}|{failed_row['write_mode']}|{failed_row['message']}\"\n",
    "            )\n",
    "        print(\"FAILED_TARGET_SUMMARY_END\")\n",
    "        failed_target_names = \", \".join(row[\"target_name\"] for row in failed_rows)\n",
    "        print(f\"FAILED_TARGET_NAMES={failed_target_names}\")\n"
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
     "nuid": "3f98102c-3505-4322-9fa9-a539de7e8c41",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Debug policy clean failure"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "debug_policy_columns=customer_id, policy_number, legacy_policy_number, product_type_code, face_amount, premium_amount, premium_frequency_code, issue_date, expiry_date, policy_status_code, beneficiary_id, rider_codes, underwriting_class_code, province, postal_code, date_of_birth, _ingested_at, _source_system, _batch_id, email_clean, email_quality_flag, phone_standardized, phone_valid_flag, phone_clean, province_clean, postal_code_clean, postal_code_valid_flag, sin_hashed, _ingested_year, _ingested_month, master_customer_id, identity_match_confidence, identity_manual_review_flag, identity_resolution_status\ndebug_policy_count=199894\nAnalysisException\n[UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE] The feature is not supported: Python UDF in the ON clause of a LEFT SEMI JOIN. In case of an INNER JOIN consider rewriting to a CROSS JOIN with a WHERE clause. SQLSTATE: 0A000\n\nJVM stacktrace:\norg.apache.spark.sql.AnalysisException\n\tat org.apache.spark.sql.errors.QueryCompilationErrors$.usePythonUDFInJoinConditionUnsupportedError(QueryCompilationErrors.scala:4229)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:333)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:142)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:2141)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:386)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:834)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Aggregate.mapChildren(basicLogicalOperators.scala:1872)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1454)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.LocalLimit.mapChildren(basicLogicalOperators.scala:2583)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.GlobalLimit.mapChildren(basicLogicalOperators.scala:2547)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:310)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$17(RuleExecutor.scala:521)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule(RuleExecutor.scala:675)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule$(RuleExecutor.scala:659)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.processRule(RuleExecutor.scala:155)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$16(RuleExecutor.scala:521)\n\tat com.databricks.spark.util.MemoryTracker$.withThreadAllocatedBytes(MemoryTracker.scala:51)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.measureRule(QueryPlanningTracker.scala:413)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$15(RuleExecutor.scala:519)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$14(RuleExecutor.scala:518)\n\tat scala.collection.immutable.ArraySeq.foldLeft(ArraySeq.scala:222)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$13(RuleExecutor.scala:510)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeBatch$1(RuleExecutor.scala:484)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23(RuleExecutor.scala:631)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23$adapted(RuleExecutor.scala:631)\n\tat scala.collection.immutable.List.foreach(List.scala:334)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:631)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:377)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.execution.QueryExecution.runOptimization$1(QueryExecution.scala:827)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$7(QueryExecution.scala:847)\n\tat com.databricks.sql.planevolution.PlanEvolutionMitigation$.optimizeAndMitigateIfRegressing(PlanEvolutionCache.scala:332)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$5(QueryExecution.scala:850)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$4(QueryExecution.scala:845)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:918)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$8(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withExecutionPhase$1(SQLExecution.scala:322)\n\tat com.databricks.util.TracingSpanUtils$.withTracing(TracingSpanUtils.scala:251)\n\tat com.databricks.spark.util.DatabricksTracingHelper.withSpan(DatabricksSparkTracingHelper.scala:154)\n\tat com.databricks.spark.util.DBRTracing$.withSpan(DBRTracing.scala:87)\n\tat org.apache.spark.sql.execution.SQLExecution$.withExecutionPhase(SQLExecution.scala:303)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$7(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:1784)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$5(QueryExecution.scala:1046)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$4(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$3(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:1042)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.localBlock$1(QueryExecution.scala:1023)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$4(QueryExecution.scala:1033)\n\tat com.databricks.unity.UCSManager$.withTemporaryScope(UCSManager.scala:168)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$3(QueryExecution.scala:1032)\n\tat org.apache.spark.sql.execution.QueryExecution$.$anonfun$runWithWrappers$2(QueryExecution.scala:2039)\n\tat org.apache.spark.sql.execution.QueryExecution$.org$apache$spark$sql$execution$QueryExecution$$runWithWrappers(QueryExecution.scala:2038)\n\tat org.apache.spark.sql.execution.QueryExecution.withQueryExecutionId(QueryExecution.scala:1033)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:1041)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:1040)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$3(QueryExecution.scala:839)\n\tat com.databricks.sql.util.MemoryTrackerHelper.withMemoryTracking(MemoryTrackerHelper.scala:111)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$1(QueryExecution.scala:836)\n\tat scala.util.Try$.apply(Try.scala:217)\n\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:874)\n\tat org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:876)\n\tat org.apache.spark.sql.connect.execution.SparkConnectPlanExecution.processAsRemoteBatches(SparkConnectPlanExecution.scala:803)\n\tat org.apache.spark.sql.connect.execution.SparkConnectPlanExecution.handlePlan(SparkConnectPlanExecution.scala:220)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.handlePlan(ExecuteThreadRunner.scala:508)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1(ExecuteThreadRunner.scala:406)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1$adapted(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:745)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:745)\n\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:744)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.executeInternal(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$execute$1(ExecuteThreadRunner.scala:196)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries(UtilizationMetrics.scala:72)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries$(UtilizationMetrics.scala:69)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.recordActiveQueries(ExecuteThreadRunner.scala:57)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.org$apache$spark$sql$connect$execution$ExecuteThreadRunner$$execute(ExecuteThreadRunner.scala:188)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$3(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$2(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n\tat com.databricks.unity.HandleImpl.$anonfun$runWithAndClose$1(UCSHandle.scala:133)\n\tat scala.util.Using$.resource(Using.scala:296)\n\tat com.databricks.unity.HandleImpl.runWithAndClose(UCSHandle.scala:132)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.run(ExecuteThreadRunner.scala:721)\n"
     ]
    },
    {
     "output_type": "stream",
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Traceback (most recent call last):\n  File \"/home/spark-cc647618-b4d0-4b4c-9755-42/.ipykernel/12367/command-7181524127550579-4168172559\", line 11, in <module>\n    display(debug_policy_df.limit(5))\n  File \"/databricks/python_shell/lib/dbruntime/display.py\", line 136, in display\n    self.display_connect_table(input, **kwargs)\n  File \"/databricks/python_shell/lib/dbruntime/display.py\", line 100, in display_connect_table\n    self.cf_helper.display_dataframe(df, config)\n  File \"/databricks/python_shell/lib/dbruntime/display_helpers/cloudfetch_helper.py\", line 48, in display_dataframe\n    display_payload = self.write_to_cloudfetch(df, config)\n                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/databricks/python_shell/lib/dbruntime/display_helpers/cloudfetch_helper.py\", line 134, in write_to_cloudfetch\n    raise e\n  File \"/databricks/python_shell/lib/dbruntime/display_helpers/cloudfetch_helper.py\", line 103, in write_to_cloudfetch\n    List[bool]] = connectDataFrame._to_cloudfetch_with_limits_and_file_paths(\n                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/dataframe.py\", line 1922, in _to_cloudfetch_with_limits_and_file_paths\n    return self._session.client.experimental_to_cloudfetch(\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py\", line 1342, in experimental_to_cloudfetch\n    for response in self._execute_and_fetch_as_iterator(req, {}, [], progress=progress):\n  File \"/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py\", line 2062, in _execute_and_fetch_as_iterator\n    self._handle_error(error)\n  File \"/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py\", line 2380, in _handle_error\n    self._handle_rpc_error(error)\n  File \"/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py\", line 2458, in _handle_rpc_error\n    raise convert_exception(\npyspark.errors.exceptions.connect.AnalysisException: [UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE] The feature is not supported: Python UDF in the ON clause of a LEFT SEMI JOIN. In case of an INNER JOIN consider rewriting to a CROSS JOIN with a WHERE clause. SQLSTATE: 0A000\n\nJVM stacktrace:\norg.apache.spark.sql.AnalysisException\n\tat org.apache.spark.sql.errors.QueryCompilationErrors$.usePythonUDFInJoinConditionUnsupportedError(QueryCompilationErrors.scala:4229)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:333)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:142)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:2141)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:386)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:834)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Aggregate.mapChildren(basicLogicalOperators.scala:1872)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1454)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.LocalLimit.mapChildren(basicLogicalOperators.scala:2583)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.GlobalLimit.mapChildren(basicLogicalOperators.scala:2547)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:310)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$17(RuleExecutor.scala:521)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule(RuleExecutor.scala:675)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule$(RuleExecutor.scala:659)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.processRule(RuleExecutor.scala:155)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$16(RuleExecutor.scala:521)\n\tat com.databricks.spark.util.MemoryTracker$.withThreadAllocatedBytes(MemoryTracker.scala:51)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.measureRule(QueryPlanningTracker.scala:413)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$15(RuleExecutor.scala:519)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$14(RuleExecutor.scala:518)\n\tat scala.collection.immutable.ArraySeq.foldLeft(ArraySeq.scala:222)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$13(RuleExecutor.scala:510)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeBatch$1(RuleExecutor.scala:484)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23(RuleExecutor.scala:631)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23$adapted(RuleExecutor.scala:631)\n\tat scala.collection.immutable.List.foreach(List.scala:334)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:631)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:377)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.execution.QueryExecution.runOptimization$1(QueryExecution.scala:827)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$7(QueryExecution.scala:847)\n\tat com.databricks.sql.planevolution.PlanEvolutionMitigation$.optimizeAndMitigateIfRegressing(PlanEvolutionCache.scala:332)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$5(QueryExecution.scala:850)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$4(QueryExecution.scala:845)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:918)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$8(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withExecutionPhase$1(SQLExecution.scala:322)\n\tat com.databricks.util.TracingSpanUtils$.withTracing(TracingSpanUtils.scala:251)\n\tat com.databricks.spark.util.DatabricksTracingHelper.withSpan(DatabricksSparkTracingHelper.scala:154)\n\tat com.databricks.spark.util.DBRTracing$.withSpan(DBRTracing.scala:87)\n\tat org.apache.spark.sql.execution.SQLExecution$.withExecutionPhase(SQLExecution.scala:303)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$7(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:1784)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$5(QueryExecution.scala:1046)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$4(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$3(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:1042)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.localBlock$1(QueryExecution.scala:1023)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$4(QueryExecution.scala:1033)\n\tat com.databricks.unity.UCSManager$.withTemporaryScope(UCSManager.scala:168)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$3(QueryExecution.scala:1032)\n\tat org.apache.spark.sql.execution.QueryExecution$.$anonfun$runWithWrappers$2(QueryExecution.scala:2039)\n\tat org.apache.spark.sql.execution.QueryExecution$.org$apache$spark$sql$execution$QueryExecution$$runWithWrappers(QueryExecution.scala:2038)\n\tat org.apache.spark.sql.execution.QueryExecution.withQueryExecutionId(QueryExecution.scala:1033)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:1041)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:1040)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$3(QueryExecution.scala:839)\n\tat com.databricks.sql.util.MemoryTrackerHelper.withMemoryTracking(MemoryTrackerHelper.scala:111)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$1(QueryExecution.scala:836)\n\tat scala.util.Try$.apply(Try.scala:217)\n\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:874)\n\tat org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:876)\n\tat org.apache.spark.sql.connect.execution.SparkConnectPlanExecution.processAsRemoteBatches(SparkConnectPlanExecution.scala:803)\n\tat org.apache.spark.sql.connect.execution.SparkConnectPlanExecution.handlePlan(SparkConnectPlanExecution.scala:220)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.handlePlan(ExecuteThreadRunner.scala:508)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1(ExecuteThreadRunner.scala:406)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1$adapted(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:745)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:745)\n\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:744)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.executeInternal(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$execute$1(ExecuteThreadRunner.scala:196)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries(UtilizationMetrics.scala:72)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries$(UtilizationMetrics.scala:69)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.recordActiveQueries(ExecuteThreadRunner.scala:57)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.org$apache$spark$sql$connect$execution$ExecuteThreadRunner$$execute(ExecuteThreadRunner.scala:188)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$3(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$2(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n\tat com.databricks.unity.HandleImpl.$anonfun$runWithAndClose$1(UCSHandle.scala:133)\n\tat scala.util.Using$.resource(Using.scala:296)\n\tat com.databricks.unity.HandleImpl.runWithAndClose(UCSHandle.scala:132)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.run(ExecuteThreadRunner.scala:721)\n"
     ]
    }
   ],
   "source": [
    "DATAFRAME_CACHE = {}\n",
    "DEDUP_AUDIT_CACHE = None\n",
    "ALLOCATION_ERROR_CACHE = None\n",
    "IDENTITY_AUTO_MERGE_CACHE = None\n",
    "IDENTITY_REVIEW_QUEUE_CACHE = None\n",
    "\n",
    "try:\n",
    "    debug_policy_df = build_target_dataframe(\"policy.individual_life_clean\")\n",
    "    print(\"debug_policy_columns=\" + \", \".join(debug_policy_df.columns))\n",
    "    print(f\"debug_policy_count={debug_policy_df.count()}\")\n",
    "    display(debug_policy_df.limit(5))\n",
    "except Exception as exc:\n",
    "    import traceback\n",
    "    print(type(exc).__name__)\n",
    "    print(str(exc))\n",
    "    traceback.print_exc(limit=20)\n"
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
     "nuid": "a1cb4cb4-42f1-4dbc-8f3e-be34e612600a",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Inspect failed execution summary"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>target_name</th><th>kind</th><th>write_mode</th><th>row_count</th><th>target_table</th><th>target_path</th><th>status</th><th>message</th></tr></thead><tbody><tr><td>policy.individual_life_enriched</td><td>business</td><td>overwrite</td><td>null</td><td>dbw_c360_canadalife.silver.policy_individual_life_enriched</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/individual_life_enriched</td><td>FAILED</td><td>AnalysisException: [UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE] The feature is not supported: Python UDF in the ON clause of a LEFT SEMI JOIN. In case of an INNER JOIN consider rewriting to a CROSS JOIN with a WHERE clause. SQLSTATE: 0A000\n",
       "\n",
       "JVM stacktrace:\n",
       "org.apache.spark.sql.AnalysisException\n",
       "\tat org.apache.spark.sql.errors.QueryCompilationErrors$.usePythonUDFInJoinConditionUnsupportedError(QueryCompilationErrors.scala:4229)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:333)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:324)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:648)\n",
       "\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:142)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:648)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat scala.collection.immutable.Vector1.map(Vector.scala:2141)\n",
       "\tat scala.collection.immutable.Vector1.map(Vector.scala:386)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:834)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Aggregate.mapChildren(basicLogicalOperators.scala:1872)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1454)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.execution.datasources.WriteFiles.mapChildren(WriteFiles.scala:60)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand.mapChildren(WriteIntoDeltaCommand.scala:46)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:324)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:310)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$17(RuleExecutor.scala:521)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule(RuleExecutor.scala:675)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule$(RuleExecutor.scala:659)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.processRule(RuleExecutor.scala:155)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$16(RuleExecutor.scala:521)\n",
       "\tat com.databricks.spark.util.MemoryTracker$.withThreadAllocatedBytes(MemoryTracker.scala:51)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.measureRule(QueryPlanningTracker.scala:413)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$15(RuleExecutor.scala:519)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$14(RuleExecutor.scala:518)\n",
       "\tat scala.collection.immutable.ArraySeq.foldLeft(ArraySeq.scala:222)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$13(RuleExecutor.scala:510)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeBatch$1(RuleExecutor.scala:484)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23(RuleExecutor.scala:631)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23$adapted(RuleExecutor.scala:631)\n",
       "\tat scala.collection.immutable.List.foreach(List.scala:334)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:631)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:377)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:365)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:365)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.runOptimization$1(QueryExecution.scala:827)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$7(QueryExecution.scala:847)\n",
       "\tat com.databricks.sql.planevolution.PlanEvolutionMitigation$.optimizeAndMitigateIfRegressing(PlanEvolutionCache.scala:332)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$5(QueryExecution.scala:850)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$4(QueryExecution.scala:845)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:918)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$8(QueryExecution.scala:1053)\n",
       "\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withExecutionPhase$1(SQLExecution.scala:322)\n",
       "\tat com.databricks.util.TracingSpanUtils$.withTracing(TracingSpanUtils.scala:251)\n",
       "\tat com.databricks.spark.util.DatabricksTracingHelper.withSpan(DatabricksSparkTracingHelper.scala:154)\n",
       "\tat com.databricks.spark.util.DBRTracing$.withSpan(DBRTracing.scala:87)\n",
       "\tat org.apache.spark.sql.execution.SQLExecution$.withExecutionPhase(SQLExecution.scala:303)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$7(QueryExecution.scala:1053)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:1784)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$5(QueryExecution.scala:1046)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$4(QueryExecution.scala:1043)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$3(QueryExecution.scala:1043)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:1042)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.localBlock$1(QueryExecution.scala:1023)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$4(QueryExecution.scala:1033)\n",
       "\tat com.databricks.unity.UCSManager$.withTemporaryScope(UCSManager.scala:168)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$3(QueryExecution.scala:1032)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution$.$anonfun$runWithWrappers$2(QueryExecution.scala:2039)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution$.org$apache$spark$sql$execution$QueryExecution$$runWithWrappers(QueryExecution.scala:2038)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.withQueryExecutionId(QueryExecution.scala:1033)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:1041)\n",
       "\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:1040)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$3(QueryExecution.scala:839)\n",
       "\tat com.databricks.sql.util.MemoryTrackerHelper.withMemoryTracking(MemoryTrackerHelper.scala:111)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$1(QueryExecution.scala:836)\n",
       "\tat scala.util.Try$.apply(Try.scala:217)\n",
       "\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n",
       "\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n",
       "\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:874)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:876)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazySparkPlan$1(QueryExecution.scala:881)\n",
       "\tat scala.util.Try$.apply(Try.scala:217)\n",
       "\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n",
       "\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n",
       "\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:947)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.executedPlanOpt(QueryExecution.scala:938)\n",
       "\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.$anonfun$onSQLExecutionEnd$1(SparkEBJCleanupListener.scala:48)\n",
       "\tat scala.Option.flatMap(Option.scala:283)\n",
       "\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onSQLExecutionEnd(SparkEBJCleanupListener.scala:48)\n",
       "\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onOtherEvent(SparkEBJCleanupListener.scala:40)\n",
       "\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent(SparkListenerBus.scala:108)\n",
       "\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent$(SparkListenerBus.scala:28)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n",
       "\tat org.apache.spark.util.ListenerBus.postToAll(ListenerBus.scala:216)\n",
       "\tat org.apache.spark.util.ListenerBus.postToAll$(ListenerBus.scala:180)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.super$postToAll(AsyncEventQueue.scala:177)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.$anonfun$dispatch$1(AsyncEventQueue.scala:177)\n",
       "\tat scala.runtime.java8.JFunction0$mcJ$sp.apply(JFunction0$mcJ$sp.scala:17)\n",
       "\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.org$apache$spark$scheduler$AsyncEventQueue$$dispatch(AsyncEventQueue.scala:119)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.$anonfun$run$1(AsyncEventQueue.scala:115)\n",
       "\tat org.apache.spark.util.Utils$.tryOrStopSparkContext(Utils.scala:1643)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.run(AsyncEventQueue.scala:115)\n",
       "\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n",
       "\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:651)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:784)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.runCommand(DataFrameWriter.scala:888)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:399)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.saveInternal(DataFrameWriter.scala:263)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.save(DataFrameWriter.scala:153)\n",
       "\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.handleWriteOperation(SparkConnectPlanner.scala:4519)\n",
       "\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.process(SparkConnectPlanner.scala:3748)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.handleCommand(ExecuteThreadRunner.scala:517)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1(ExecuteThreadRunner.scala:405)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1$adapted(ExecuteThreadRunner.scala:331)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:745)\n",
       "\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:745)\n",
       "\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:744)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.executeInternal(ExecuteThreadRunner.scala:331)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$execute$1(ExecuteThreadRunner.scala:196)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries(UtilizationMetrics.scala:72)\n",
       "\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries$(UtilizationMetrics.scala:69)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.recordActiveQueries(ExecuteThreadRunner.scala:57)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.org$apache$spark$sql$connect$execution$ExecuteThreadRunner$$execute(ExecuteThreadRunner.scala:188)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$3(ExecuteThreadRunner.scala:722)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$2(ExecuteThreadRunner.scala:722)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n",
       "\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n",
       "\tat com.databricks.unity.HandleImpl.$anonfun$runWithAndClose$1(UCSHandle.scala:133)\n",
       "\tat scala.util.Using$.resource(Using.scala:296)\n",
       "\tat com.databricks.unity.HandleImpl.runWithAndClose(UCSHandle.scala:132)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.run(ExecuteThreadRunner.scala:721)</td></tr><tr><td>policy.policy_rider_detail</td><td>business</td><td>overwrite</td><td>null</td><td>dbw_c360_canadalife.silver.policy_policy_rider_detail</td><td>abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/policy_rider_detail</td><td>FAILED</td><td>AnalysisException: [UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE] The feature is not supported: Python UDF in the ON clause of a LEFT SEMI JOIN. In case of an INNER JOIN consider rewriting to a CROSS JOIN with a WHERE clause. SQLSTATE: 0A000\n",
       "\n",
       "JVM stacktrace:\n",
       "org.apache.spark.sql.AnalysisException\n",
       "\tat org.apache.spark.sql.errors.QueryCompilationErrors$.usePythonUDFInJoinConditionUnsupportedError(QueryCompilationErrors.scala:4229)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:333)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:324)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:648)\n",
       "\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:142)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:648)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat scala.collection.immutable.Vector1.map(Vector.scala:2141)\n",
       "\tat scala.collection.immutable.Vector1.map(Vector.scala:386)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:834)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Aggregate.mapChildren(basicLogicalOperators.scala:1872)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1454)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Generate.mapChildren(basicLogicalOperators.scala:346)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Filter.mapChildren(basicLogicalOperators.scala:392)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n",
       "\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat org.apache.spark.sql.execution.datasources.WriteFiles.mapChildren(WriteFiles.scala:60)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n",
       "\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n",
       "\tat com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand.mapChildren(WriteIntoDeltaCommand.scala:46)\n",
       "\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n",
       "\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:324)\n",
       "\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:310)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$17(RuleExecutor.scala:521)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule(RuleExecutor.scala:675)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule$(RuleExecutor.scala:659)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.processRule(RuleExecutor.scala:155)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$16(RuleExecutor.scala:521)\n",
       "\tat com.databricks.spark.util.MemoryTracker$.withThreadAllocatedBytes(MemoryTracker.scala:51)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.measureRule(QueryPlanningTracker.scala:413)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$15(RuleExecutor.scala:519)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$14(RuleExecutor.scala:518)\n",
       "\tat scala.collection.immutable.ArraySeq.foldLeft(ArraySeq.scala:222)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$13(RuleExecutor.scala:510)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeBatch$1(RuleExecutor.scala:484)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23(RuleExecutor.scala:631)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23$adapted(RuleExecutor.scala:631)\n",
       "\tat scala.collection.immutable.List.foreach(List.scala:334)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:631)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:377)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:365)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n",
       "\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:365)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.runOptimization$1(QueryExecution.scala:827)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$7(QueryExecution.scala:847)\n",
       "\tat com.databricks.sql.planevolution.PlanEvolutionMitigation$.optimizeAndMitigateIfRegressing(PlanEvolutionCache.scala:332)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$5(QueryExecution.scala:850)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$4(QueryExecution.scala:845)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n",
       "\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n",
       "\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n",
       "\tat org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:918)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$8(QueryExecution.scala:1053)\n",
       "\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withExecutionPhase$1(SQLExecution.scala:322)\n",
       "\tat com.databricks.util.TracingSpanUtils$.withTracing(TracingSpanUtils.scala:251)\n",
       "\tat com.databricks.spark.util.DatabricksTracingHelper.withSpan(DatabricksSparkTracingHelper.scala:154)\n",
       "\tat com.databricks.spark.util.DBRTracing$.withSpan(DBRTracing.scala:87)\n",
       "\tat org.apache.spark.sql.execution.SQLExecution$.withExecutionPhase(SQLExecution.scala:303)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$7(QueryExecution.scala:1053)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:1784)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$5(QueryExecution.scala:1046)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$4(QueryExecution.scala:1043)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$3(QueryExecution.scala:1043)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:1042)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.localBlock$1(QueryExecution.scala:1023)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$4(QueryExecution.scala:1033)\n",
       "\tat com.databricks.unity.UCSManager$.withTemporaryScope(UCSManager.scala:168)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$3(QueryExecution.scala:1032)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution$.$anonfun$runWithWrappers$2(QueryExecution.scala:2039)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution$.org$apache$spark$sql$execution$QueryExecution$$runWithWrappers(QueryExecution.scala:2038)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.withQueryExecutionId(QueryExecution.scala:1033)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:1041)\n",
       "\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:1040)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$3(QueryExecution.scala:839)\n",
       "\tat com.databricks.sql.util.MemoryTrackerHelper.withMemoryTracking(MemoryTrackerHelper.scala:111)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$1(QueryExecution.scala:836)\n",
       "\tat scala.util.Try$.apply(Try.scala:217)\n",
       "\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n",
       "\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n",
       "\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:874)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:876)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazySparkPlan$1(QueryExecution.scala:881)\n",
       "\tat scala.util.Try$.apply(Try.scala:217)\n",
       "\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n",
       "\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n",
       "\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:947)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.executedPlanOpt(QueryExecution.scala:938)\n",
       "\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.$anonfun$onSQLExecutionEnd$1(SparkEBJCleanupListener.scala:48)\n",
       "\tat scala.Option.flatMap(Option.scala:283)\n",
       "\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onSQLExecutionEnd(SparkEBJCleanupListener.scala:48)\n",
       "\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onOtherEvent(SparkEBJCleanupListener.scala:40)\n",
       "\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent(SparkListenerBus.scala:108)\n",
       "\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent$(SparkListenerBus.scala:28)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n",
       "\tat org.apache.spark.util.ListenerBus.postToAll(ListenerBus.scala:216)\n",
       "\tat org.apache.spark.util.ListenerBus.postToAll$(ListenerBus.scala:180)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.super$postToAll(AsyncEventQueue.scala:177)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.$anonfun$dispatch$1(AsyncEventQueue.scala:177)\n",
       "\tat scala.runtime.java8.JFunction0$mcJ$sp.apply(JFunction0$mcJ$sp.scala:17)\n",
       "\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue.org$apache$spark$scheduler$AsyncEventQueue$$dispatch(AsyncEventQueue.scala:119)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.$anonfun$run$1(AsyncEventQueue.scala:115)\n",
       "\tat org.apache.spark.util.Utils$.tryOrStopSparkContext(Utils.scala:1643)\n",
       "\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.run(AsyncEventQueue.scala:115)\n",
       "\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n",
       "\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:651)\n",
       "\tat org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:784)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.runCommand(DataFrameWriter.scala:888)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:399)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.saveInternal(DataFrameWriter.scala:263)\n",
       "\tat org.apache.spark.sql.classic.DataFrameWriter.save(DataFrameWriter.scala:153)\n",
       "\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.handleWriteOperation(SparkConnectPlanner.scala:4519)\n",
       "\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.process(SparkConnectPlanner.scala:3748)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.handleCommand(ExecuteThreadRunner.scala:517)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1(ExecuteThreadRunner.scala:405)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1$adapted(ExecuteThreadRunner.scala:331)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:745)\n",
       "\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:745)\n",
       "\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:744)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.executeInternal(ExecuteThreadRunner.scala:331)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$execute$1(ExecuteThreadRunner.scala:196)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries(UtilizationMetrics.scala:72)\n",
       "\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries$(UtilizationMetrics.scala:69)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.recordActiveQueries(ExecuteThreadRunner.scala:57)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.org$apache$spark$sql$connect$execution$ExecuteThreadRunner$$execute(ExecuteThreadRunner.scala:188)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$3(ExecuteThreadRunner.scala:722)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$2(ExecuteThreadRunner.scala:722)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n",
       "\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n",
       "\tat com.databricks.unity.HandleImpl.$anonfun$runWithAndClose$1(UCSHandle.scala:133)\n",
       "\tat scala.util.Using$.resource(Using.scala:296)\n",
       "\tat com.databricks.unity.HandleImpl.runWithAndClose(UCSHandle.scala:132)\n",
       "\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.run(ExecuteThreadRunner.scala:721)</td></tr></tbody></table></div>"
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
         "policy.individual_life_enriched",
         "business",
         "overwrite",
         null,
         "dbw_c360_canadalife.silver.policy_individual_life_enriched",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/individual_life_enriched",
         "FAILED",
         "AnalysisException: [UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE] The feature is not supported: Python UDF in the ON clause of a LEFT SEMI JOIN. In case of an INNER JOIN consider rewriting to a CROSS JOIN with a WHERE clause. SQLSTATE: 0A000\n\nJVM stacktrace:\norg.apache.spark.sql.AnalysisException\n\tat org.apache.spark.sql.errors.QueryCompilationErrors$.usePythonUDFInJoinConditionUnsupportedError(QueryCompilationErrors.scala:4229)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:333)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:142)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:2141)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:386)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:834)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Aggregate.mapChildren(basicLogicalOperators.scala:1872)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1454)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.execution.datasources.WriteFiles.mapChildren(WriteFiles.scala:60)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand.mapChildren(WriteIntoDeltaCommand.scala:46)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:310)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$17(RuleExecutor.scala:521)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule(RuleExecutor.scala:675)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule$(RuleExecutor.scala:659)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.processRule(RuleExecutor.scala:155)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$16(RuleExecutor.scala:521)\n\tat com.databricks.spark.util.MemoryTracker$.withThreadAllocatedBytes(MemoryTracker.scala:51)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.measureRule(QueryPlanningTracker.scala:413)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$15(RuleExecutor.scala:519)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$14(RuleExecutor.scala:518)\n\tat scala.collection.immutable.ArraySeq.foldLeft(ArraySeq.scala:222)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$13(RuleExecutor.scala:510)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeBatch$1(RuleExecutor.scala:484)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23(RuleExecutor.scala:631)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23$adapted(RuleExecutor.scala:631)\n\tat scala.collection.immutable.List.foreach(List.scala:334)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:631)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:377)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.execution.QueryExecution.runOptimization$1(QueryExecution.scala:827)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$7(QueryExecution.scala:847)\n\tat com.databricks.sql.planevolution.PlanEvolutionMitigation$.optimizeAndMitigateIfRegressing(PlanEvolutionCache.scala:332)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$5(QueryExecution.scala:850)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$4(QueryExecution.scala:845)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:918)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$8(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withExecutionPhase$1(SQLExecution.scala:322)\n\tat com.databricks.util.TracingSpanUtils$.withTracing(TracingSpanUtils.scala:251)\n\tat com.databricks.spark.util.DatabricksTracingHelper.withSpan(DatabricksSparkTracingHelper.scala:154)\n\tat com.databricks.spark.util.DBRTracing$.withSpan(DBRTracing.scala:87)\n\tat org.apache.spark.sql.execution.SQLExecution$.withExecutionPhase(SQLExecution.scala:303)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$7(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:1784)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$5(QueryExecution.scala:1046)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$4(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$3(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:1042)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.localBlock$1(QueryExecution.scala:1023)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$4(QueryExecution.scala:1033)\n\tat com.databricks.unity.UCSManager$.withTemporaryScope(UCSManager.scala:168)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$3(QueryExecution.scala:1032)\n\tat org.apache.spark.sql.execution.QueryExecution$.$anonfun$runWithWrappers$2(QueryExecution.scala:2039)\n\tat org.apache.spark.sql.execution.QueryExecution$.org$apache$spark$sql$execution$QueryExecution$$runWithWrappers(QueryExecution.scala:2038)\n\tat org.apache.spark.sql.execution.QueryExecution.withQueryExecutionId(QueryExecution.scala:1033)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:1041)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:1040)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$3(QueryExecution.scala:839)\n\tat com.databricks.sql.util.MemoryTrackerHelper.withMemoryTracking(MemoryTrackerHelper.scala:111)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$1(QueryExecution.scala:836)\n\tat scala.util.Try$.apply(Try.scala:217)\n\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:874)\n\tat org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:876)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazySparkPlan$1(QueryExecution.scala:881)\n\tat scala.util.Try$.apply(Try.scala:217)\n\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:947)\n\tat org.apache.spark.sql.execution.QueryExecution.executedPlanOpt(QueryExecution.scala:938)\n\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.$anonfun$onSQLExecutionEnd$1(SparkEBJCleanupListener.scala:48)\n\tat scala.Option.flatMap(Option.scala:283)\n\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onSQLExecutionEnd(SparkEBJCleanupListener.scala:48)\n\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onOtherEvent(SparkEBJCleanupListener.scala:40)\n\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent(SparkListenerBus.scala:108)\n\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent$(SparkListenerBus.scala:28)\n\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n\tat org.apache.spark.util.ListenerBus.postToAll(ListenerBus.scala:216)\n\tat org.apache.spark.util.ListenerBus.postToAll$(ListenerBus.scala:180)\n\tat org.apache.spark.scheduler.AsyncEventQueue.super$postToAll(AsyncEventQueue.scala:177)\n\tat org.apache.spark.scheduler.AsyncEventQueue.$anonfun$dispatch$1(AsyncEventQueue.scala:177)\n\tat scala.runtime.java8.JFunction0$mcJ$sp.apply(JFunction0$mcJ$sp.scala:17)\n\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n\tat org.apache.spark.scheduler.AsyncEventQueue.org$apache$spark$scheduler$AsyncEventQueue$$dispatch(AsyncEventQueue.scala:119)\n\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.$anonfun$run$1(AsyncEventQueue.scala:115)\n\tat org.apache.spark.util.Utils$.tryOrStopSparkContext(Utils.scala:1643)\n\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.run(AsyncEventQueue.scala:115)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:651)\n\tat org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:784)\n\tat org.apache.spark.sql.classic.DataFrameWriter.runCommand(DataFrameWriter.scala:888)\n\tat org.apache.spark.sql.classic.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:399)\n\tat org.apache.spark.sql.classic.DataFrameWriter.saveInternal(DataFrameWriter.scala:263)\n\tat org.apache.spark.sql.classic.DataFrameWriter.save(DataFrameWriter.scala:153)\n\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.handleWriteOperation(SparkConnectPlanner.scala:4519)\n\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.process(SparkConnectPlanner.scala:3748)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.handleCommand(ExecuteThreadRunner.scala:517)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1(ExecuteThreadRunner.scala:405)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1$adapted(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:745)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:745)\n\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:744)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.executeInternal(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$execute$1(ExecuteThreadRunner.scala:196)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries(UtilizationMetrics.scala:72)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries$(UtilizationMetrics.scala:69)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.recordActiveQueries(ExecuteThreadRunner.scala:57)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.org$apache$spark$sql$connect$execution$ExecuteThreadRunner$$execute(ExecuteThreadRunner.scala:188)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$3(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$2(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n\tat com.databricks.unity.HandleImpl.$anonfun$runWithAndClose$1(UCSHandle.scala:133)\n\tat scala.util.Using$.resource(Using.scala:296)\n\tat com.databricks.unity.HandleImpl.runWithAndClose(UCSHandle.scala:132)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.run(ExecuteThreadRunner.scala:721)"
        ],
        [
         "policy.policy_rider_detail",
         "business",
         "overwrite",
         null,
         "dbw_c360_canadalife.silver.policy_policy_rider_detail",
         "abfss://silver@adlsc360canadalife.dfs.core.windows.net/dbw_c360_canadalife/silver/policy/policy_rider_detail",
         "FAILED",
         "AnalysisException: [UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE] The feature is not supported: Python UDF in the ON clause of a LEFT SEMI JOIN. In case of an INNER JOIN consider rewriting to a CROSS JOIN with a WHERE clause. SQLSTATE: 0A000\n\nJVM stacktrace:\norg.apache.spark.sql.AnalysisException\n\tat org.apache.spark.sql.errors.QueryCompilationErrors$.usePythonUDFInJoinConditionUnsupportedError(QueryCompilationErrors.scala:4229)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:333)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$$anonfun$apply$6.applyOrElse(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$2(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:142)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:648)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:2141)\n\tat scala.collection.immutable.Vector1.map(Vector.scala:386)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:834)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Aggregate.mapChildren(basicLogicalOperators.scala:1872)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1454)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Generate.mapChildren(basicLogicalOperators.scala:346)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Filter.mapChildren(basicLogicalOperators.scala:392)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren(TreeNode.scala:1452)\n\tat org.apache.spark.sql.catalyst.trees.BinaryLike.mapChildren$(TreeNode.scala:1451)\n\tat org.apache.spark.sql.catalyst.plans.logical.Join.mapChildren(basicLogicalOperators.scala:1016)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.catalyst.plans.logical.Project.mapChildren(basicLogicalOperators.scala:95)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat org.apache.spark.sql.execution.datasources.WriteFiles.mapChildren(WriteFiles.scala:60)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUpWithPruning$1(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren(TreeNode.scala:1425)\n\tat org.apache.spark.sql.catalyst.trees.UnaryLike.mapChildren$(TreeNode.scala:1424)\n\tat com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand.mapChildren(WriteIntoDeltaCommand.scala:46)\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformUpWithPruning(TreeNode.scala:645)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning(AnalysisHelper.scala:374)\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformUpWithPruning$(AnalysisHelper.scala:369)\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformUpWithPruning(LogicalPlan.scala:48)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:324)\n\tat org.apache.spark.sql.catalyst.optimizer.ExtractPythonUDFFromJoinCondition$.apply(joins.scala:310)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$17(RuleExecutor.scala:521)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule(RuleExecutor.scala:675)\n\tat org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule$(RuleExecutor.scala:659)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.processRule(RuleExecutor.scala:155)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$16(RuleExecutor.scala:521)\n\tat com.databricks.spark.util.MemoryTracker$.withThreadAllocatedBytes(MemoryTracker.scala:51)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.measureRule(QueryPlanningTracker.scala:413)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$15(RuleExecutor.scala:519)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$14(RuleExecutor.scala:518)\n\tat scala.collection.immutable.ArraySeq.foldLeft(ArraySeq.scala:222)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$13(RuleExecutor.scala:510)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeBatch$1(RuleExecutor.scala:484)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23(RuleExecutor.scala:631)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$23$adapted(RuleExecutor.scala:631)\n\tat scala.collection.immutable.List.foreach(List.scala:334)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:631)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:377)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:365)\n\tat org.apache.spark.sql.execution.QueryExecution.runOptimization$1(QueryExecution.scala:827)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$7(QueryExecution.scala:847)\n\tat com.databricks.sql.planevolution.PlanEvolutionMitigation$.optimizeAndMitigateIfRegressing(PlanEvolutionCache.scala:332)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$5(QueryExecution.scala:850)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:266)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$4(QueryExecution.scala:845)\n\tat com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)\n\tat com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)\n\tat com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)\n\tat org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:918)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$8(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withExecutionPhase$1(SQLExecution.scala:322)\n\tat com.databricks.util.TracingSpanUtils$.withTracing(TracingSpanUtils.scala:251)\n\tat com.databricks.spark.util.DatabricksTracingHelper.withSpan(DatabricksSparkTracingHelper.scala:154)\n\tat com.databricks.spark.util.DBRTracing$.withSpan(DBRTracing.scala:87)\n\tat org.apache.spark.sql.execution.SQLExecution$.withExecutionPhase(SQLExecution.scala:303)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$7(QueryExecution.scala:1053)\n\tat org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:1784)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$5(QueryExecution.scala:1046)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$4(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$3(QueryExecution.scala:1043)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:1042)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.sql.execution.QueryExecution.localBlock$1(QueryExecution.scala:1023)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$4(QueryExecution.scala:1033)\n\tat com.databricks.unity.UCSManager$.withTemporaryScope(UCSManager.scala:168)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$withQueryExecutionId$3(QueryExecution.scala:1032)\n\tat org.apache.spark.sql.execution.QueryExecution$.$anonfun$runWithWrappers$2(QueryExecution.scala:2039)\n\tat org.apache.spark.sql.execution.QueryExecution$.org$apache$spark$sql$execution$QueryExecution$$runWithWrappers(QueryExecution.scala:2038)\n\tat org.apache.spark.sql.execution.QueryExecution.withQueryExecutionId(QueryExecution.scala:1033)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:1041)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:1040)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$3(QueryExecution.scala:839)\n\tat com.databricks.sql.util.MemoryTrackerHelper.withMemoryTracking(MemoryTrackerHelper.scala:111)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazyOptimizedPlan$1(QueryExecution.scala:836)\n\tat scala.util.Try$.apply(Try.scala:217)\n\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:874)\n\tat org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:876)\n\tat org.apache.spark.sql.execution.QueryExecution.$anonfun$lazySparkPlan$1(QueryExecution.scala:881)\n\tat scala.util.Try$.apply(Try.scala:217)\n\tat org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1770)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:947)\n\tat org.apache.spark.sql.execution.QueryExecution.executedPlanOpt(QueryExecution.scala:938)\n\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.$anonfun$onSQLExecutionEnd$1(SparkEBJCleanupListener.scala:48)\n\tat scala.Option.flatMap(Option.scala:283)\n\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onSQLExecutionEnd(SparkEBJCleanupListener.scala:48)\n\tat com.databricks.spark.sql.execution.SparkEBJCleanupListener.onOtherEvent(SparkEBJCleanupListener.scala:40)\n\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent(SparkListenerBus.scala:108)\n\tat org.apache.spark.scheduler.SparkListenerBus.doPostEvent$(SparkListenerBus.scala:28)\n\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n\tat org.apache.spark.scheduler.AsyncEventQueue.doPostEvent(AsyncEventQueue.scala:46)\n\tat org.apache.spark.util.ListenerBus.postToAll(ListenerBus.scala:216)\n\tat org.apache.spark.util.ListenerBus.postToAll$(ListenerBus.scala:180)\n\tat org.apache.spark.scheduler.AsyncEventQueue.super$postToAll(AsyncEventQueue.scala:177)\n\tat org.apache.spark.scheduler.AsyncEventQueue.$anonfun$dispatch$1(AsyncEventQueue.scala:177)\n\tat scala.runtime.java8.JFunction0$mcJ$sp.apply(JFunction0$mcJ$sp.scala:17)\n\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n\tat org.apache.spark.scheduler.AsyncEventQueue.org$apache$spark$scheduler$AsyncEventQueue$$dispatch(AsyncEventQueue.scala:119)\n\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.$anonfun$run$1(AsyncEventQueue.scala:115)\n\tat org.apache.spark.util.Utils$.tryOrStopSparkContext(Utils.scala:1643)\n\tat org.apache.spark.scheduler.AsyncEventQueue$$anon$2.run(AsyncEventQueue.scala:115)\n\tat org.apache.spark.util.Utils$.getTryWithCallerStacktrace(Utils.scala:1820)\n\tat org.apache.spark.util.LazyTry.get(LazyTry.scala:78)\n\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:651)\n\tat org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:784)\n\tat org.apache.spark.sql.classic.DataFrameWriter.runCommand(DataFrameWriter.scala:888)\n\tat org.apache.spark.sql.classic.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:399)\n\tat org.apache.spark.sql.classic.DataFrameWriter.saveInternal(DataFrameWriter.scala:263)\n\tat org.apache.spark.sql.classic.DataFrameWriter.save(DataFrameWriter.scala:153)\n\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.handleWriteOperation(SparkConnectPlanner.scala:4519)\n\tat org.apache.spark.sql.connect.planner.SparkConnectPlanner.process(SparkConnectPlanner.scala:3748)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.handleCommand(ExecuteThreadRunner.scala:517)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1(ExecuteThreadRunner.scala:405)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$executeInternal$1$adapted(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:745)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:745)\n\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:744)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.executeInternal(ExecuteThreadRunner.scala:331)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.$anonfun$execute$1(ExecuteThreadRunner.scala:196)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries(UtilizationMetrics.scala:72)\n\tat com.databricks.spark.connect.service.UtilizationMetrics.recordActiveQueries$(UtilizationMetrics.scala:69)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.recordActiveQueries(ExecuteThreadRunner.scala:57)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner.org$apache$spark$sql$connect$execution$ExecuteThreadRunner$$execute(ExecuteThreadRunner.scala:188)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$3(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.$anonfun$run$2(ExecuteThreadRunner.scala:722)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n\tat com.databricks.unity.HandleImpl.$anonfun$runWithAndClose$1(UCSHandle.scala:133)\n\tat scala.util.Using$.resource(Using.scala:296)\n\tat com.databricks.unity.HandleImpl.runWithAndClose(UCSHandle.scala:132)\n\tat org.apache.spark.sql.connect.execution.ExecuteThreadRunner$ExecutionThread.run(ExecuteThreadRunner.scala:721)"
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
         "name": "target_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "kind",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "write_mode",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "row_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "target_table",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "target_path",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "message",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "failed_target_count=2\nenriched_duplicate_columns=\nenriched_columns=premium_frequency_code, customer_id, policy_number, legacy_policy_number, product_type_code, face_amount, premium_amount, issue_date, expiry_date, policy_status_code, beneficiary_id, rider_codes, underwriting_class_code, province, postal_code, date_of_birth, _ingested_at, _source_system, _batch_id, email_clean, email_quality_flag, phone_standardized, phone_valid_flag, phone_clean, province_clean, postal_code_clean, postal_code_valid_flag, sin_hashed, _ingested_year, _ingested_month, master_customer_id, identity_match_confidence, identity_manual_review_flag, identity_resolution_status, annualised_premium, product_category, product_type_code_canonical, policy_status_canonical, policy_tenure_days, term_expiry_days_remaining, term_expiring_90d_flag, churn_risk_signal\n"
     ]
    }
   ],
   "source": [
    "failed_rows = [row for row in result_rows if row.get(\"status\") == \"FAILED\"]\n",
    "failed_rows_schema = T.StructType([\n",
    "    T.StructField(\"target_name\", T.StringType(), True),\n",
    "    T.StructField(\"kind\", T.StringType(), True),\n",
    "    T.StructField(\"write_mode\", T.StringType(), True),\n",
    "    T.StructField(\"row_count\", T.LongType(), True),\n",
    "    T.StructField(\"target_table\", T.StringType(), True),\n",
    "    T.StructField(\"target_path\", T.StringType(), True),\n",
    "    T.StructField(\"status\", T.StringType(), True),\n",
    "    T.StructField(\"message\", T.StringType(), True),\n",
    "])\n",
    "failed_rows_df = spark.createDataFrame(failed_rows, failed_rows_schema)\n",
    "display(failed_rows_df)\n",
    "print(f\"failed_target_count={len(failed_rows)}\")\n",
    "\n",
    "DATAFRAME_CACHE = {}\n",
    "DEDUP_AUDIT_CACHE = None\n",
    "ALLOCATION_ERROR_CACHE = None\n",
    "enriched_df = build_target_dataframe(\"policy.individual_life_enriched\")\n",
    "duplicate_columns = sorted({column_name for column_name in enriched_df.columns if enriched_df.columns.count(column_name) > 1})\n",
    "print(\"enriched_duplicate_columns=\" + \", \".join(duplicate_columns))\n",
    "print(\"enriched_columns=\" + \", \".join(enriched_df.columns))"
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
     "nuid": "0a245609-6df3-4bc3-aab0-beede6cb77a5",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Silver expectation spot checks"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>table_name</th><th>exists</th><th>row_count</th></tr></thead><tbody><tr><td>customer_master</td><td>true</td><td>300000</td></tr><tr><td>digital_portal_clean</td><td>true</td><td>200000</td></tr><tr><td>freedom55_advisor_feed_clean</td><td>true</td><td>100000</td></tr><tr><td>group_benefits_certificate_clean</td><td>true</td><td>99956</td></tr><tr><td>group_benefits_certificate_coverage_detail</td><td>true</td><td>250021</td></tr><tr><td>group_benefits_plan_clean</td><td>true</td><td>100000</td></tr><tr><td>group_retirement_member_clean</td><td>true</td><td>99954</td></tr><tr><td>interactions_callcentre_clean</td><td>true</td><td>100000</td></tr><tr><td>investments_climl_clean</td><td>true</td><td>100000</td></tr><tr><td>investments_fund_allocation_detail</td><td>true</td><td>100000</td></tr><tr><td>monitoring_allocation_errors</td><td>true</td><td>0</td></tr><tr><td>monitoring_dedup_audit_log</td><td>true</td><td>0</td></tr><tr><td>monitoring_schema_drift_log</td><td>true</td><td>143</td></tr><tr><td>policy_disability_ci_clean</td><td>true</td><td>19887</td></tr><tr><td>policy_individual_life_clean</td><td>true</td><td>199894</td></tr><tr><td>policy_individual_life_enriched</td><td>true</td><td>199894</td></tr><tr><td>policy_policy_rider_detail</td><td>true</td><td>99669</td></tr><tr><td>reference_product_code_mapping</td><td>true</td><td>25</td></tr><tr><td>reference_rider_codes</td><td>true</td><td>2</td></tr><tr><td>reference_status_code_mapping</td><td>true</td><td>17</td></tr><tr><td>reinsurance_treaty_clean</td><td>true</td><td>10000</td></tr></tbody></table></div>"
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
         "customer_master",
         true,
         300000
        ],
        [
         "digital_portal_clean",
         true,
         200000
        ],
        [
         "freedom55_advisor_feed_clean",
         true,
         100000
        ],
        [
         "group_benefits_certificate_clean",
         true,
         99956
        ],
        [
         "group_benefits_certificate_coverage_detail",
         true,
         250021
        ],
        [
         "group_benefits_plan_clean",
         true,
         100000
        ],
        [
         "group_retirement_member_clean",
         true,
         99954
        ],
        [
         "interactions_callcentre_clean",
         true,
         100000
        ],
        [
         "investments_climl_clean",
         true,
         100000
        ],
        [
         "investments_fund_allocation_detail",
         true,
         100000
        ],
        [
         "monitoring_allocation_errors",
         true,
         0
        ],
        [
         "monitoring_dedup_audit_log",
         true,
         0
        ],
        [
         "monitoring_schema_drift_log",
         true,
         143
        ],
        [
         "policy_disability_ci_clean",
         true,
         19887
        ],
        [
         "policy_individual_life_clean",
         true,
         199894
        ],
        [
         "policy_individual_life_enriched",
         true,
         199894
        ],
        [
         "policy_policy_rider_detail",
         true,
         99669
        ],
        [
         "reference_product_code_mapping",
         true,
         25
        ],
        [
         "reference_rider_codes",
         true,
         2
        ],
        [
         "reference_status_code_mapping",
         true,
         17
        ],
        [
         "reinsurance_treaty_clean",
         true,
         10000
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
         "name": "table_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "exists",
         "type": "\"boolean\""
        },
        {
         "metadata": "{}",
         "name": "row_count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>check_name</th><th>passed</th></tr></thead><tbody><tr><td>all_21_tables_present</td><td>true</td></tr><tr><td>allocation_detail_generated</td><td>true</td></tr><tr><td>callcentre_duration_present</td><td>true</td></tr><tr><td>customer_masked_email_present</td><td>true</td></tr><tr><td>customer_masked_phone_present</td><td>true</td></tr><tr><td>customer_postal_normalized</td><td>true</td></tr><tr><td>customer_province_two_letter</td><td>true</td></tr><tr><td>digital_contains_both_sources</td><td>true</td></tr><tr><td>freedom55_assignment_status_present</td><td>true</td></tr><tr><td>group_retirement_status_present</td><td>true</td></tr><tr><td>identity_resolution_columns_present</td><td>true</td></tr><tr><td>manual_review_queue_present</td><td>true</td></tr><tr><td>monitoring_schema_drift_generated</td><td>true</td></tr><tr><td>policy_canonical_product_present</td><td>true</td></tr><tr><td>policy_canonical_status_present</td><td>true</td></tr><tr><td>policy_current_rows_present</td><td>true</td></tr><tr><td>policy_effective_date_matches_run_date</td><td>true</td></tr><tr><td>policy_scd2_columns_present</td><td>true</td></tr><tr><td>product_reference_generated</td><td>true</td></tr><tr><td>rider_detail_generated</td><td>true</td></tr><tr><td>status_reference_generated</td><td>true</td></tr></tbody></table></div>"
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
         "all_21_tables_present",
         true
        ],
        [
         "allocation_detail_generated",
         true
        ],
        [
         "callcentre_duration_present",
         true
        ],
        [
         "customer_masked_email_present",
         true
        ],
        [
         "customer_masked_phone_present",
         true
        ],
        [
         "customer_postal_normalized",
         true
        ],
        [
         "customer_province_two_letter",
         true
        ],
        [
         "digital_contains_both_sources",
         true
        ],
        [
         "freedom55_assignment_status_present",
         true
        ],
        [
         "group_retirement_status_present",
         true
        ],
        [
         "identity_resolution_columns_present",
         true
        ],
        [
         "manual_review_queue_present",
         true
        ],
        [
         "monitoring_schema_drift_generated",
         true
        ],
        [
         "policy_canonical_product_present",
         true
        ],
        [
         "policy_canonical_status_present",
         true
        ],
        [
         "policy_current_rows_present",
         true
        ],
        [
         "policy_effective_date_matches_run_date",
         true
        ],
        [
         "policy_scd2_columns_present",
         true
        ],
        [
         "product_reference_generated",
         true
        ],
        [
         "rider_detail_generated",
         true
        ],
        [
         "status_reference_generated",
         true
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
         "name": "check_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "passed",
         "type": "\"boolean\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>table_name</th><th>sample_value</th></tr></thead><tbody><tr><td>customer_master</td><td>{'customer_id': '00001abe-2ccb-4a44-9398-9eb26c329ebc', 'email_clean': None, 'phone_clean': None, 'province_clean': None, 'postal_code_clean': None}</td></tr><tr><td>policy_individual_life_clean</td><td>{'policy_number': 'GWL-62860330', 'legacy_policy_number': 'GWL-62860330', 'product_type_code': 'Group Benefits', 'policy_status_code': 'Pending', 'beneficiary_id': None, 'rider_codes': None, 'underwriting_class_code': None, 'effective_date': datetime.date(2026, 6, 8), 'expiry_date': None, 'is_current': True}</td></tr><tr><td>policy_individual_life_enriched</td><td>{'policy_number': 'GWL-62860330', 'product_type_code': 'Group Benefits', 'product_type_code_canonical': 'GB', 'policy_status_code': 'Pending', 'policy_status_canonical': 'P', 'annualised_premium': Decimal('6989.35'), 'churn_risk_signal': 'LOW'}</td></tr><tr><td>digital_portal_clean</td><td>{'event_id': '0000abebb1890f69db4fbe57494b18b756e75534f23f57efdb4d41b95de91fe1', 'customer_id': 'fbdae79a-6575-4636-b91d-86dbc0321dc5', 'event_type': 'Medical', '_source_system': 'my_cl_portal'}</td></tr><tr><td>interactions_callcentre_clean</td><td>{'interaction_id': 'INT-92308810', 'customer_id': 'e4600c23-32ca-4bf0-9d62-cb68d2f3af94', 'agent_id': 'AGT-3666', 'interaction_status': 'Escalated', 'call_duration_minutes': 50.766666666666666}</td></tr><tr><td>freedom55_advisor_feed_clean</td><td>{'assignment_id': 'ASN-11669735', 'advisor_id': 'ADV-0000', 'customer_id': '50a6ffb0-2837-42a2-bc7e-c067fc0c650b', 'advisor_assignment_status': 'Active', 'region': 'Quebec', 'channel': 'Online'}</td></tr><tr><td>group_retirement_member_clean</td><td>{'member_id': 'GRM-00000011', 'plan_id': 'GRP-6244', 'employer_id': 'EMP-793236', 'contribution_amount': Decimal('7825.30'), 'vesting_status': 'Partially Vested', 'member_status': 'Retired'}</td></tr></tbody></table></div>"
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
         "customer_master",
         "{'customer_id': '00001abe-2ccb-4a44-9398-9eb26c329ebc', 'email_clean': None, 'phone_clean': None, 'province_clean': None, 'postal_code_clean': None}"
        ],
        [
         "policy_individual_life_clean",
         "{'policy_number': 'GWL-62860330', 'legacy_policy_number': 'GWL-62860330', 'product_type_code': 'Group Benefits', 'policy_status_code': 'Pending', 'beneficiary_id': None, 'rider_codes': None, 'underwriting_class_code': None, 'effective_date': datetime.date(2026, 6, 8), 'expiry_date': None, 'is_current': True}"
        ],
        [
         "policy_individual_life_enriched",
         "{'policy_number': 'GWL-62860330', 'product_type_code': 'Group Benefits', 'product_type_code_canonical': 'GB', 'policy_status_code': 'Pending', 'policy_status_canonical': 'P', 'annualised_premium': Decimal('6989.35'), 'churn_risk_signal': 'LOW'}"
        ],
        [
         "digital_portal_clean",
         "{'event_id': '0000abebb1890f69db4fbe57494b18b756e75534f23f57efdb4d41b95de91fe1', 'customer_id': 'fbdae79a-6575-4636-b91d-86dbc0321dc5', 'event_type': 'Medical', '_source_system': 'my_cl_portal'}"
        ],
        [
         "interactions_callcentre_clean",
         "{'interaction_id': 'INT-92308810', 'customer_id': 'e4600c23-32ca-4bf0-9d62-cb68d2f3af94', 'agent_id': 'AGT-3666', 'interaction_status': 'Escalated', 'call_duration_minutes': 50.766666666666666}"
        ],
        [
         "freedom55_advisor_feed_clean",
         "{'assignment_id': 'ASN-11669735', 'advisor_id': 'ADV-0000', 'customer_id': '50a6ffb0-2837-42a2-bc7e-c067fc0c650b', 'advisor_assignment_status': 'Active', 'region': 'Quebec', 'channel': 'Online'}"
        ],
        [
         "group_retirement_member_clean",
         "{'member_id': 'GRM-00000011', 'plan_id': 'GRP-6244', 'employer_id': 'EMP-793236', 'contribution_amount': Decimal('7825.30'), 'vesting_status': 'Partially Vested', 'member_status': 'Retired'}"
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
         "name": "table_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "sample_value",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DIGITAL_SOURCE_SYSTEMS=adobe_analytics.digital_events, my_cl_portal\nFAILED_EXPECTATION_CHECKS=NONE\nSILVER_EXPECTATION_STATUS=PASS\n"
     ]
    }
   ],
   "source": [
    "# ------------------------------------------------------------------------------\n",
    "# 7. Silver expectation spot checks\n",
    "# ------------------------------------------------------------------------------\n",
    "spot_check_run_date = run_date or \"2026-06-08\"\n",
    "silver_tables_expected = [\n",
    "    \"customer_master\",\n",
    "    \"digital_portal_clean\",\n",
    "    \"freedom55_advisor_feed_clean\",\n",
    "    \"group_benefits_plan_clean\",\n",
    "    \"group_benefits_certificate_clean\",\n",
    "    \"group_benefits_certificate_coverage_detail\",\n",
    "    \"group_retirement_member_clean\",\n",
    "    \"interactions_callcentre_clean\",\n",
    "    \"investments_climl_clean\",\n",
    "    \"investments_fund_allocation_detail\",\n",
    "    \"monitoring_allocation_errors\",\n",
    "    \"monitoring_dedup_audit_log\",\n",
    "    \"monitoring_schema_drift_log\",\n",
    "    \"policy_disability_ci_clean\",\n",
    "    \"policy_individual_life_clean\",\n",
    "    \"policy_individual_life_enriched\",\n",
    "    \"policy_policy_rider_detail\",\n",
    "    \"reference_product_code_mapping\",\n",
    "    \"reference_rider_codes\",\n",
    "    \"reference_status_code_mapping\",\n",
    "    \"reinsurance_treaty_clean\",\n",
    "]\n",
    "\n",
    "summary_rows = []\n",
    "for table_name in silver_tables_expected:\n",
    "    fqn = f\"{catalog_name}.{silver_schema}.{table_name}\"\n",
    "    exists_flag = table_exists(fqn)\n",
    "    row_count = spark.table(fqn).count() if exists_flag else None\n",
    "    summary_rows.append((table_name, exists_flag, row_count))\n",
    "\n",
    "summary_df = spark.createDataFrame(summary_rows, [\"table_name\", \"exists\", \"row_count\"])\n",
    "display(summary_df.orderBy(\"table_name\"))\n",
    "\n",
    "customer_df = spark.table(silver_table_fqn(\"customer.master\"))\n",
    "policy_clean_df = spark.table(silver_table_fqn(\"policy.individual_life_clean\"))\n",
    "policy_enriched_df = spark.table(silver_table_fqn(\"policy.individual_life_enriched\"))\n",
    "digital_df = spark.table(silver_table_fqn(\"digital.portal_clean\"))\n",
    "call_df = spark.table(silver_table_fqn(\"interactions.callcentre_clean\"))\n",
    "f55_df = spark.table(silver_table_fqn(\"freedom55.advisor_feed_clean\"))\n",
    "retirement_df = spark.table(silver_table_fqn(\"group_retirement.member_clean\"))\n",
    "rider_df = spark.table(silver_table_fqn(\"policy.policy_rider_detail\"))\n",
    "alloc_df = spark.table(silver_table_fqn(\"investments.fund_allocation_detail\"))\n",
    "status_ref_df = spark.table(silver_table_fqn(\"reference.status_code_mapping\"))\n",
    "product_ref_df = spark.table(silver_table_fqn(\"reference.product_code_mapping\"))\n",
    "drift_df = spark.table(silver_table_fqn(\"monitoring.schema_drift_log\"))\n",
    "\n",
    "checks = [\n",
    "    (\"all_21_tables_present\", summary_df.filter(~F.col(\"exists\")).count() == 0),\n",
    "    (\"customer_masked_email_present\", customer_df.filter(F.col(\"email_clean\").rlike(r\"^..\\*\\*\\*@\")).count() > 0),\n",
    "    (\"customer_masked_phone_present\", customer_df.filter(F.col(\"phone_clean\").rlike(r\"^\\*\\*\\*-\\*\\*\\*-\\d{4}$\")).count() > 0),\n",
    "    (\"customer_postal_normalized\", customer_df.filter(F.col(\"postal_code_clean\").contains(\" \")).count() == 0),\n",
    "    (\"customer_province_two_letter\", customer_df.filter((F.col(\"province_clean\").isNotNull()) & (F.length(F.col(\"province_clean\")) != 2)).count() == 0),\n",
    "    (\"policy_scd2_columns_present\", set([\"effective_date\", \"expiry_date\", \"is_current\"]).issubset(set(policy_clean_df.columns))),\n",
    "    (\"policy_current_rows_present\", policy_clean_df.filter(F.col(\"is_current\") == True).count() > 0),\n",
    "    (\"policy_effective_date_matches_run_date\", policy_clean_df.filter(F.col(\"effective_date\") == F.to_date(F.lit(spot_check_run_date))).count() > 0),\n",
    "    (\"policy_canonical_product_present\", policy_enriched_df.filter(F.col(\"product_type_code_canonical\").isNotNull()).count() > 0),\n",
    "    (\"policy_canonical_status_present\", policy_enriched_df.filter(F.col(\"policy_status_canonical\").isNotNull()).count() > 0),\n",
    "    (\"digital_contains_both_sources\", digital_df.select(\"_source_system\").distinct().count() >= 2),\n",
    "    (\"callcentre_duration_present\", call_df.filter(F.col(\"call_duration_minutes\").isNotNull()).count() > 0),\n",
    "    (\"freedom55_assignment_status_present\", f55_df.filter(F.col(\"advisor_assignment_status\").isNotNull()).count() > 0),\n",
    "    (\"group_retirement_status_present\", retirement_df.filter(F.col(\"member_status\").isNotNull()).count() > 0),\n",
    "    (\"rider_detail_generated\", rider_df.count() > 0),\n",
    "    (\"allocation_detail_generated\", alloc_df.count() > 0),\n",
    "    (\"status_reference_generated\", status_ref_df.count() > 0),\n",
    "    (\"product_reference_generated\", product_ref_df.count() > 0),\n",
    "    (\"monitoring_schema_drift_generated\", drift_df.count() >= 0),\n",
    "    (\"identity_resolution_columns_present\", set([\"postal_code_clean\", \"date_of_birth\"]).issubset(set(policy_clean_df.columns))),\n",
    "    (\"manual_review_queue_present\", table_exists(f\"{catalog_name}.compliance.identity_resolution_manual_review\")),\n",
    "]\n",
    "\n",
    "checks_df = spark.createDataFrame(checks, [\"check_name\", \"passed\"])\n",
    "display(checks_df.orderBy(\"check_name\"))\n",
    "\n",
    "samples = [\n",
    "    (\"customer_master\", str(customer_df.select(\"customer_id\", \"email_clean\", \"phone_clean\", \"province_clean\", \"postal_code_clean\").limit(1).collect()[0].asDict())),\n",
    "    (\"policy_individual_life_clean\", str(policy_clean_df.select(\"policy_number\", \"legacy_policy_number\", \"product_type_code\", \"policy_status_code\", \"beneficiary_id\", \"rider_codes\", \"underwriting_class_code\", \"effective_date\", \"expiry_date\", \"is_current\").limit(1).collect()[0].asDict())),\n",
    "    (\"policy_individual_life_enriched\", str(policy_enriched_df.select(\"policy_number\", \"product_type_code\", \"product_type_code_canonical\", \"policy_status_code\", \"policy_status_canonical\", \"annualised_premium\", \"churn_risk_signal\").limit(1).collect()[0].asDict())),\n",
    "    (\"digital_portal_clean\", str(digital_df.select(\"event_id\", \"customer_id\", \"event_type\", \"_source_system\").limit(1).collect()[0].asDict())),\n",
    "    (\"interactions_callcentre_clean\", str(call_df.select(\"interaction_id\", \"customer_id\", \"agent_id\", \"interaction_status\", \"call_duration_minutes\").limit(1).collect()[0].asDict())),\n",
    "    (\"freedom55_advisor_feed_clean\", str(f55_df.select(\"assignment_id\", \"advisor_id\", \"customer_id\", \"advisor_assignment_status\", \"region\", \"channel\").limit(1).collect()[0].asDict())),\n",
    "    (\"group_retirement_member_clean\", str(retirement_df.select(\"member_id\", \"plan_id\", \"employer_id\", \"contribution_amount\", \"vesting_status\", \"member_status\").limit(1).collect()[0].asDict())),\n",
    "]\n",
    "samples_df = spark.createDataFrame(samples, [\"table_name\", \"sample_value\"])\n",
    "display(samples_df)\n",
    "\n",
    "failed_checks = [row[0] for row in checks_df.filter(F.col(\"passed\") == False).collect()]\n",
    "print(\"DIGITAL_SOURCE_SYSTEMS=\" + \", \".join(sorted([row[0] for row in digital_df.select(\"_source_system\").distinct().collect()])))\n",
    "print(\"FAILED_EXPECTATION_CHECKS=\" + (\", \".join(failed_checks) if failed_checks else \"NONE\"))\n",
    "if failed_checks:\n",
    "    print(\"SILVER_EXPECTATION_STATUS=PARTIAL\")\n",
    "else:\n",
    "    print(\"SILVER_EXPECTATION_STATUS=PASS\")\n"
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
     "nuid": "1b7ae88a-3423-4836-94ab-ca74b0fa335f",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Debug downstream silver targets"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DEBUG_TARGET=policy.individual_life_clean\nNameError\nname 'build_target_dataframe' is not defined\nDEBUG_TARGET=policy.individual_life_enriched\nNameError\nname 'build_target_dataframe' is not defined\nDEBUG_TARGET=policy.policy_rider_detail\nNameError\nname 'build_target_dataframe' is not defined\n"
     ]
    },
    {
     "output_type": "stream",
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Traceback (most recent call last):\n  File \"/home/spark-bb93f3d1-b839-4738-b5e2-60/.ipykernel/2048/command-7181524127550596-3412768768\", line 9, in <module>\n    debug_target_df = build_target_dataframe(debug_target_name)\n                      ^^^^^^^^^^^^^^^^^^^^^^\nNameError: name 'build_target_dataframe' is not defined\nTraceback (most recent call last):\n  File \"/home/spark-bb93f3d1-b839-4738-b5e2-60/.ipykernel/2048/command-7181524127550596-3412768768\", line 9, in <module>\n    debug_target_df = build_target_dataframe(debug_target_name)\n                      ^^^^^^^^^^^^^^^^^^^^^^\nNameError: name 'build_target_dataframe' is not defined\nTraceback (most recent call last):\n  File \"/home/spark-bb93f3d1-b839-4738-b5e2-60/.ipykernel/2048/command-7181524127550596-3412768768\", line 9, in <module>\n    debug_target_df = build_target_dataframe(debug_target_name)\n                      ^^^^^^^^^^^^^^^^^^^^^^\nNameError: name 'build_target_dataframe' is not defined\n"
     ]
    }
   ],
   "source": [
    "DATAFRAME_CACHE = {}\n",
    "DEDUP_AUDIT_CACHE = None\n",
    "ALLOCATION_ERROR_CACHE = None\n",
    "IDENTITY_AUTO_MERGE_CACHE = None\n",
    "IDENTITY_REVIEW_QUEUE_CACHE = None\n",
    "\n",
    "for debug_target_name in [\"policy.individual_life_clean\", \"policy.individual_life_enriched\", \"policy.policy_rider_detail\"]:\n",
    "    try:\n",
    "        debug_target_df = build_target_dataframe(debug_target_name)\n",
    "        print(f\"DEBUG_TARGET={debug_target_name}\")\n",
    "        print(\"debug_target_columns=\" + \", \".join(debug_target_df.columns))\n",
    "        print(f\"debug_target_count={debug_target_df.count()}\")\n",
    "        try:\n",
    "            debug_preview_rows = debug_target_df.limit(5).collect()\n",
    "            debug_preview_df = spark.createDataFrame(debug_preview_rows, debug_target_df.schema) if debug_preview_rows else spark.createDataFrame([], debug_target_df.schema)\n",
    "            display(debug_preview_df)\n",
    "        except Exception as preview_exc:\n",
    "            import traceback\n",
    "            print(f\"DEBUG_PREVIEW_FAILED={debug_target_name}\")\n",
    "            print(type(preview_exc).__name__)\n",
    "            print(str(preview_exc))\n",
    "            traceback.print_exc(limit=20)\n",
    "    except Exception as exc:\n",
    "        import traceback\n",
    "        print(f\"DEBUG_TARGET={debug_target_name}\")\n",
    "        print(type(exc).__name__)\n",
    "        print(str(exc))\n",
    "        traceback.print_exc(limit=20)\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "Silver_Batch_Processing_Engine",
   "widgets": {
    "dq_threshold_pct": {
     "currentValue": "2.0",
     "nuid": "a0ab1289-919d-482d-9742-feecb98c9e40",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "2.0",
      "label": null,
      "name": "dq_threshold_pct",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "2.0",
      "label": null,
      "name": "dq_threshold_pct",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "execution_mode": {
     "currentValue": "RUN",
     "nuid": "0a936af2-41cb-4f36-8dc2-dc62b9fa4616",
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
    "optimize_output": {
     "currentValue": "false",
     "nuid": "16292ef0-0d0d-49f1-a456-b1c1f056d2e7",
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
    "run_date": {
     "currentValue": "2026-06-08",
     "nuid": "9a0533c8-5d9c-437e-9467-c9c40c594ef2",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "",
      "label": null,
      "name": "run_date",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "",
      "label": null,
      "name": "run_date",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "target_table_name": {
     "currentValue": "ALL",
     "nuid": "21099a0e-6961-4a06-8f1d-4a87fc02b87c",
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
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
