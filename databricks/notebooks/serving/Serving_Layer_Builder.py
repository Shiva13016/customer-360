# Databricks notebook source
# MAGIC %md
# MAGIC # Serving_Layer_Builder
# MAGIC
# MAGIC **Layer:** Serving
# MAGIC **Purpose:** Builds serving layer views/feature tables from Gold data.
# MAGIC
# MAGIC **Source path:** `/Users/shivakumaryallanti5@gmail.com/project customer 360/Serving_Layer_Builder`

# COMMAND ----------

print("Serving Layer Builder - creating serving tables...")

# COMMAND ----------

{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a4d7767c-f559-4c79-b209-226f1afec433",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Canada Life C360 Serving Layer Builder"
    }
   },
   "source": [
    "# Canada Life C360 Serving Layer Builder\n",
    "\n",
    "## Purpose\n",
    "Builds the `dbw_c360_canadalife.serving` schema with dimensional and fact tables optimized for BI consumption:\n",
    "\n",
    "* **dim_customer**: Customer dimension (one row per customer, current state)\n",
    "* **fact_kpi_daily**: Daily KPI fact from gold.kpi_summary\n",
    "* **fact_advisor_book**: Advisor book fact from gold.book_of_business\n",
    "* **fact_interactions**: Call centre interactions fact from silver.interactions_callcentre_clean\n",
    "\n",
    "## Architecture\n",
    "* **Source**: Gold layer (`customer_360`, `ml_features`, `kpi_summary`, `book_of_business`) + Silver layer (`interactions_callcentre_clean`)\n",
    "* **Target**: `dbw_c360_canadalife.serving` schema\n",
    "* **Write Mode**: Overwrite (full refresh)\n",
    "* **Metadata**: `serving_ingested_at`, `serving_run_id`\n",
    "\n",
    "## Execution Modes\n",
    "* **PLAN**: Show execution plan without creating tables\n",
    "* **TEST**: Create tables, show sample output, no production write\n",
    "* **RUN**: Full production execution\n",
    "\n",
    "## Parameters\n",
    "* `execution_mode`: PLAN / TEST / RUN\n",
    "* `catalog_name`: Unity Catalog name (default: dbw_c360_canadalife)\n",
    "* `target_table_name`: Table to build (default: ALL)\n",
    "\n",
    "---\n",
    "**Recommended first run**: `dim_customer` and `fact_kpi_daily` (cleanest sources)"
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
     "nuid": "0ff5349c-a304-450d-bb07-13a5c3b254c0",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Imports and Setup"
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import functions as F\n",
    "import json\n",
    "import uuid"
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
     "nuid": "898d0644-1d8a-41e2-b186-d112602017e1",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Widgets"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Execution Mode: RUN\nCatalog: dbw_c360_canadalife\nServing Schema: dbw_c360_canadalife.serving\nTarget Table: ALL\nRun ID: 7cb15a86-2bfd-420f-b65f-7ff50d4e1f0c\n"
     ]
    }
   ],
   "source": [
    "dbutils.widgets.dropdown(\"execution_mode\", \"PLAN\", [\"PLAN\", \"TEST\", \"RUN\"], \"Execution Mode\")\n",
    "dbutils.widgets.text(\"catalog_name\", \"dbw_c360_canadalife\", \"Catalog Name\")\n",
    "dbutils.widgets.text(\"target_table_name\", \"ALL\", \"Target Table Name\")\n",
    "\n",
    "execution_mode = dbutils.widgets.get(\"execution_mode\")\n",
    "catalog_name = dbutils.widgets.get(\"catalog_name\")\n",
    "target_table_name = dbutils.widgets.get(\"target_table_name\").upper()\n",
    "\n",
    "serving_schema = f\"{catalog_name}.serving\"\n",
    "serving_run_id = str(uuid.uuid4())\n",
    "serving_ingested_at = F.current_timestamp()\n",
    "\n",
    "print(f\"Execution Mode: {execution_mode}\")\n",
    "print(f\"Catalog: {catalog_name}\")\n",
    "print(f\"Serving Schema: {serving_schema}\")\n",
    "print(f\"Target Table: {target_table_name}\")\n",
    "print(f\"Run ID: {serving_run_id}\")"
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
     "nuid": "de10dadd-0304-4a63-acf1-714544977366",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Create Serving Schema"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✓ Schema dbw_c360_canadalife.serving ready\n"
     ]
    }
   ],
   "source": [
    "if execution_mode in [\"TEST\", \"RUN\"]:\n",
    "    spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {serving_schema}\")\n",
    "    print(f\"✓ Schema {serving_schema} ready\")\n",
    "else:\n",
    "    print(f\"[PLAN] Would create schema: {serving_schema}\")"
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
     "nuid": "4cfafcfb-1599-4b23-88f4-f1d2eaf48dfe",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Helper Functions"
    }
   },
   "outputs": [],
   "source": [
    "def add_serving_metadata(df):\n",
    "    \"\"\"Add serving layer metadata columns\"\"\"\n",
    "    return df.withColumns({\n",
    "        \"serving_ingested_at\": serving_ingested_at,\n",
    "        \"serving_run_id\": F.lit(serving_run_id)\n",
    "    })\n",
    "\n",
    "table_results = []\n",
    "\n",
    "\n",
    "def write_serving_table(df, table_name, description=\"\"):\n",
    "    \"\"\"Write serving table with metadata and execution mode handling\"\"\"\n",
    "    full_table_name = f\"{serving_schema}.{table_name}\"\n",
    "\n",
    "    # Add metadata\n",
    "    df_with_meta = add_serving_metadata(df)\n",
    "\n",
    "    if execution_mode == \"PLAN\":\n",
    "        row_count = df_with_meta.count()\n",
    "        print(f\"\\n[PLAN] {full_table_name}\")\n",
    "        print(f\"  Description: {description}\")\n",
    "        print(f\"  Row count estimate: {row_count:,}\")\n",
    "        print(\"  Schema:\")\n",
    "        df_with_meta.printSchema()\n",
    "        table_results.append({\n",
    "            \"table_name\": table_name,\n",
    "            \"full_table_name\": full_table_name,\n",
    "            \"mode\": execution_mode,\n",
    "            \"status\": \"planned\",\n",
    "            \"row_count\": row_count,\n",
    "            \"description\": description\n",
    "        })\n",
    "        return None\n",
    "\n",
    "    if execution_mode == \"TEST\":\n",
    "        row_count = df_with_meta.count()\n",
    "        print(f\"\\n[TEST] {full_table_name}\")\n",
    "        print(f\"  Description: {description}\")\n",
    "        print(f\"  Row count: {row_count:,}\")\n",
    "        print(\"  Sample (5 rows):\")\n",
    "        display(df_with_meta.limit(5))\n",
    "        table_results.append({\n",
    "            \"table_name\": table_name,\n",
    "            \"full_table_name\": full_table_name,\n",
    "            \"mode\": execution_mode,\n",
    "            \"status\": \"tested\",\n",
    "            \"row_count\": row_count,\n",
    "            \"description\": description\n",
    "        })\n",
    "        return df_with_meta\n",
    "\n",
    "    if execution_mode == \"RUN\":\n",
    "        print(f\"\\n[RUN] Writing {full_table_name}...\")\n",
    "        df_with_meta.write \\\n",
    "            .format(\"delta\") \\\n",
    "            .mode(\"overwrite\") \\\n",
    "            .option(\"overwriteSchema\", \"true\") \\\n",
    "            .saveAsTable(full_table_name)\n",
    "\n",
    "        row_count = spark.table(full_table_name).count()\n",
    "        print(f\"  ✓ Written: {row_count:,} rows\")\n",
    "        print(f\"  Table: {full_table_name}\")\n",
    "        table_results.append({\n",
    "            \"table_name\": table_name,\n",
    "            \"full_table_name\": full_table_name,\n",
    "            \"mode\": execution_mode,\n",
    "            \"status\": \"written\",\n",
    "            \"row_count\": row_count,\n",
    "            \"description\": description\n",
    "        })\n",
    "        return df_with_meta\n",
    "\n",
    "    raise ValueError(f\"Unsupported execution_mode: {execution_mode}\")\n",
    "\n",
    "def should_build(table_name):\n",
    "    \"\"\"Check if table should be built based on target_table_name parameter\"\"\"\n",
    "    return target_table_name == \"ALL\" or target_table_name == table_name.upper()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "5e56f173-e72c-4140-82d0-ad93038847ba",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "dim_customer Builder"
    }
   },
   "source": [
    "## dim_customer: Customer Dimension\n",
    "\n",
    "**Source**: `gold.customer_360` + `gold.ml_features`  \n",
    "**Grain**: One row per customer (current state)  \n",
    "**Join**: `customer_360.customer_id = ml_features.c360_customer_id`"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8ba197b9-6900-4433-8788-bca08346d238",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "fact_interactions Builder"
    }
   },
   "source": [
    "## fact_interactions: Call Centre Interactions Fact\n",
    "\n",
    "**Source**: `silver.interactions_callcentre_clean`  \n",
    "**Grain**: One row per interaction"
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
     "nuid": "b04c4f03-479f-43ef-9759-b61109dfae3e",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Build fact_interactions"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n[RUN] Writing dbw_c360_canadalife.serving.fact_interactions...\n  ✓ Written: 100,000 rows\n  Table: dbw_c360_canadalife.serving.fact_interactions\n"
     ]
    }
   ],
   "source": [
    "if should_build(\"fact_interactions\"):\n",
    "    # Read silver source\n",
    "    interactions = spark.table(f\"{catalog_name}.silver.interactions_callcentre_clean\")\n",
    "    \n",
    "    # Build fact\n",
    "    fact_interactions = interactions.select(\n",
    "        \"interaction_id\",\n",
    "        \"customer_id\",\n",
    "        \"agent_id\",\n",
    "        \"call_start_ts\",\n",
    "        \"call_end_ts\",\n",
    "        \"channel\",\n",
    "        \"issue_type\",\n",
    "        \"interaction_status\",\n",
    "        \"call_duration_minutes\"\n",
    "    )\n",
    "    \n",
    "    write_serving_table(\n",
    "        fact_interactions,\n",
    "        \"fact_interactions\",\n",
    "        \"Call centre interactions fact from silver.interactions_callcentre_clean\"\n",
    "    )\n",
    "else:\n",
    "    print(\"[SKIP] fact_interactions\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e2f6db6a-3844-4a26-8f75-71ad86e1b64b",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "fact_advisor_book Builder"
    }
   },
   "source": [
    "## fact_advisor_book: Advisor Book Fact\n",
    "\n",
    "**Source**: `gold.book_of_business`  \n",
    "**Grain**: One row per customer-policy-advisor assignment"
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
     "nuid": "69a5bc9f-740f-4851-93a7-e83b0c992990",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Build fact_advisor_book"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n[RUN] Writing dbw_c360_canadalife.serving.fact_advisor_book...\n  ✓ Written: 33,401 rows\n  Table: dbw_c360_canadalife.serving.fact_advisor_book\n"
     ]
    }
   ],
   "source": [
    "if should_build(\"fact_advisor_book\"):\n",
    "    # Read gold source\n",
    "    book_of_business = spark.table(f\"{catalog_name}.gold.book_of_business\")\n",
    "    \n",
    "    # Build fact\n",
    "    fact_advisor_book = book_of_business.select(\n",
    "        \"customer_id\",\n",
    "        \"policy_number\",\n",
    "        \"advisor_id\",\n",
    "        \"assignment_start_date\",\n",
    "        \"region\",\n",
    "        \"channel\",\n",
    "        \"product_type_code_canonical\",\n",
    "        \"annualised_premium\",\n",
    "        \"churn_risk_signal\",\n",
    "        \"term_expiring_90d_flag\",\n",
    "        \"disability_gap_flag\",\n",
    "        \"ci_cross_sell_flag\"\n",
    "    )\n",
    "    \n",
    "    write_serving_table(\n",
    "        fact_advisor_book,\n",
    "        \"fact_advisor_book\",\n",
    "        \"Advisor book fact from gold.book_of_business\"\n",
    "    )\n",
    "else:\n",
    "    print(\"[SKIP] fact_advisor_book\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "fd7c2810-ca23-4185-83e1-124197850a87",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "fact_kpi_daily Builder"
    }
   },
   "source": [
    "## fact_kpi_daily: Daily KPI Fact\n",
    "\n",
    "**Source**: `gold.kpi_summary`  \n",
    "**Grain**: One row per date  \n",
    "**Note**: This is already a clean daily grain fact table"
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
     "nuid": "e83dbfd7-c60d-4262-b8ed-c1be5396d743",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Build fact_kpi_daily"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n[RUN] Writing dbw_c360_canadalife.serving.fact_kpi_daily...\n  ✓ Written: 6,964 rows\n  Table: dbw_c360_canadalife.serving.fact_kpi_daily\n"
     ]
    }
   ],
   "source": [
    "if should_build(\"fact_kpi_daily\"):\n",
    "    # Read gold source\n",
    "    kpi_summary = spark.table(f\"{catalog_name}.gold.kpi_summary\")\n",
    "    \n",
    "    # Build fact (pass-through with metadata)\n",
    "    fact_kpi_daily = kpi_summary.select(\n",
    "        \"kpi_date\",\n",
    "        \"active_customers\",\n",
    "        \"churned_customers\",\n",
    "        \"churn_rate_30d\",\n",
    "        \"policies_issued\",\n",
    "        \"total_premium_written\",\n",
    "        \"total_face_amount_issued\"\n",
    "    )\n",
    "    \n",
    "    write_serving_table(\n",
    "        fact_kpi_daily,\n",
    "        \"fact_kpi_daily\",\n",
    "        \"Daily KPI fact from gold.kpi_summary\"\n",
    "    )\n",
    "else:\n",
    "    print(\"[SKIP] fact_kpi_daily\")"
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
     "nuid": "f76aa0c0-5b73-4325-b5a3-59abfb338e66",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Build dim_customer"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n[RUN] Writing dbw_c360_canadalife.serving.dim_customer...\n  ✓ Written: 100,000 rows\n  Table: dbw_c360_canadalife.serving.dim_customer\n"
     ]
    }
   ],
   "source": [
    "if should_build(\"dim_customer\"):\n",
    "    # Read gold sources\n",
    "    c360 = spark.table(f\"{catalog_name}.gold.customer_360\")\n",
    "    ml_features = spark.table(f\"{catalog_name}.gold.ml_features\")\n",
    "    \n",
    "    # Build dimension\n",
    "    dim_customer = c360.select(\n",
    "        \"customer_id\",\n",
    "        \"first_name\",\n",
    "        \"last_name\",\n",
    "        \"email_clean\",\n",
    "        \"phone_clean\",\n",
    "        \"province_clean\",\n",
    "        \"postal_code_clean\",\n",
    "        \"channel\",\n",
    "        \"advisor_id\",\n",
    "        \"first_policy_date\",\n",
    "        \"life_policy_count\",\n",
    "        \"total_life_face_amount\",\n",
    "        \"total_life_premium\",\n",
    "        \"has_term_expiring_90d\",\n",
    "        \"has_disability_coverage\",\n",
    "        \"has_ci_coverage\",\n",
    "        \"group_benefits_plan_count\",\n",
    "        \"total_retirement_contribution\",\n",
    "        \"total_investment_market_value\",\n",
    "        \"last_callcentre_contact\",\n",
    "        \"callcentre_interaction_count\",\n",
    "        \"cross_sell_propensity_ci\",\n",
    "        \"cross_sell_term_conversion_flag\"\n",
    "    ).join(\n",
    "        ml_features.select(\n",
    "            F.col(\"c360_customer_id\").alias(\"customer_id\"),\n",
    "            \"digital_engagement_score\",\n",
    "            \"logins_count\",\n",
    "            \"doc_downloads_count\",\n",
    "            \"has_retirement_account\"\n",
    "        ),\n",
    "        on=\"customer_id\",\n",
    "        how=\"left\"\n",
    "    )\n",
    "    \n",
    "    write_serving_table(\n",
    "        dim_customer,\n",
    "        \"dim_customer\",\n",
    "        \"Customer dimension combining customer_360 and ml_features\"\n",
    "    )\n",
    "else:\n",
    "    print(\"[SKIP] dim_customer\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "928af38f-90b8-4ea3-b8af-8859c7374377",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Execution Summary"
    }
   },
   "source": [
    "## Execution Summary\n",
    "\n",
    "All serving layer tables have been processed based on the selected execution mode and target table filter."
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
     "nuid": "63627c21-d5f6-4e2e-8f2d-6bcc64716120",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Completion Log"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "============================================================\nServing Layer Build Complete\n============================================================\nExecution Mode: RUN\nCatalog: dbw_c360_canadalife\nSchema: dbw_c360_canadalife.serving\nTarget Table Filter: ALL\nRun ID: 7cb15a86-2bfd-420f-b65f-7ff50d4e1f0c\n============================================================\n\nServing tables created:\n"
     ]
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>database</th><th>tableName</th><th>isTemporary</th></tr></thead><tbody><tr><td>serving</td><td>dim_customer</td><td>false</td></tr><tr><td>serving</td><td>fact_advisor_book</td><td>false</td></tr><tr><td>serving</td><td>fact_interactions</td><td>false</td></tr><tr><td>serving</td><td>fact_kpi_daily</td><td>false</td></tr></tbody></table></div>"
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
         "serving",
         "dim_customer",
         false
        ],
        [
         "serving",
         "fact_advisor_book",
         false
        ],
        [
         "serving",
         "fact_interactions",
         false
        ],
        [
         "serving",
         "fact_kpi_daily",
         false
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
         "name": "database",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "tableName",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "isTemporary",
         "type": "\"boolean\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "print(\"=\"*60)\n",
    "print(\"Serving Layer Build Complete\")\n",
    "print(\"=\"*60)\n",
    "print(f\"Execution Mode: {execution_mode}\")\n",
    "print(f\"Catalog: {catalog_name}\")\n",
    "print(f\"Schema: {serving_schema}\")\n",
    "print(f\"Target Table Filter: {target_table_name}\")\n",
    "print(f\"Run ID: {serving_run_id}\")\n",
    "print(\"=\"*60)\n",
    "\n",
    "if table_results:\n",
    "    job_summary_df = spark.createDataFrame(table_results)\n",
    "    print(\"\\nProcessed tables summary:\")\n",
    "    display(job_summary_df)\n",
    "    print(\"\\nJOB_SUMMARY_JSON=\" + json.dumps(table_results, sort_keys=True))\n",
    "\n",
    "if execution_mode == \"RUN\":\n",
    "    print(\"\\nServing tables created:\")\n",
    "    serving_tables = spark.sql(f\"SHOW TABLES IN {serving_schema}\").filter(\"isTemporary = false\")\n",
    "    display(serving_tables)"
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
   "notebookName": "Serving_Layer_Builder",
   "widgets": {
    "catalog_name": {
     "currentValue": "dbw_c360_canadalife",
     "nuid": "a868483a-e2b2-42e3-8594-392fba58206a",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "dbw_c360_canadalife",
      "label": "Catalog Name",
      "name": "catalog_name",
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
      "label": "Catalog Name",
      "name": "catalog_name",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "execution_mode": {
     "currentValue": "RUN",
     "nuid": "7633ea2b-c61a-4608-a94f-3c1dec7f26bc",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "PLAN",
      "label": "Execution Mode",
      "name": "execution_mode",
      "options": {
       "widgetDisplayType": "Dropdown",
       "choices": [
        "PLAN",
        "TEST",
        "RUN"
       ],
       "fixedDomain": true,
       "multiselect": false
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "dropdown",
      "defaultValue": "PLAN",
      "label": "Execution Mode",
      "name": "execution_mode",
      "options": {
       "widgetType": "dropdown",
       "autoCreated": null,
       "choices": [
        "PLAN",
        "TEST",
        "RUN"
       ]
      }
     }
    },
    "target_table_name": {
     "currentValue": "ALL",
     "nuid": "1daf3f78-8c4c-47bd-8040-096524bc5548",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "ALL",
      "label": "Target Table Name",
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
      "label": "Target Table Name",
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
