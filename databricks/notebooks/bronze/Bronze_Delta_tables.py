# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze_Delta_tables
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Purpose:** Ingests raw data into Bronze Delta tables.
# MAGIC
# MAGIC **Source path:** `/Users/shivakumaryallanti5@gmail.com/project customer 360/Bronze_Delta_tables`

# COMMAND ----------

dbutils.widgets.text("source", "", "Source Name")
source = dbutils.widgets.get("source")
print(f"Loading source into Bronze Delta: {source}")

# COMMAND ----------


{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "ead4245f-9bbb-4b23-8a62-52c68d1e3fdd",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Cell 1"
    }
   },
   "source": [
    "# Bronze to Delta append pipeline\n",
    "\n",
    "This notebook loads one Bronze source folder from ADLS and appends it into a Unity Catalog Delta table.\n",
    "\n",
    "It is split into small steps so the flow is easy to follow:\n",
    "\n",
    "* define imports and project defaults\n",
    "* read runtime parameters\n",
    "* build the target table name\n",
    "* build and validate the source path\n",
    "* read source files\n",
    "* add bronze metadata columns\n",
    "* append the data into the target Delta table"
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
     "nuid": "3badae42-2896-451f-8873-20fc232399d4",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Imports and defaults"
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import col, current_timestamp, lit\n",
    "from datetime import datetime, date\n",
    "import re\n",
    "\n",
    "catalog = \"dbw_c360_canadalife\"\n",
    "schema = \"bronze\"\n",
    "supported_formats = {\"csv\", \"json\", \"parquet\"}\n",
    "SOURCE_CONTRACTS = {\n",
    "    \"salesforce_crm\": {\"expected_prefixes\": [\"crm_customers_\"], \"expected_format\": \"csv\"},\n",
    "    \"gwl_policy_admin\": {\"expected_prefixes\": [\"gwl_policies_\"], \"expected_format\": \"csv\"},\n",
    "    \"ll_policy_admin\": {\"expected_prefixes\": [\"policies_\"], \"expected_format\": \"csv\"},\n",
    "    \"sap_billing\": {\"expected_prefixes\": [\"billing_invoices_\"], \"expected_format\": \"csv\"},\n",
    "    \"adobe_analytics\": {\"expected_prefixes\": [\"web_sessions_\"], \"expected_format\": \"json\"},\n",
    "    \"avaya_call_centre\": {\"expected_prefixes\": [\"call_centre_interactions_\", \"interactions_\"], \"expected_format\": \"csv\"},\n",
    "    \"group_benefits\": {\"expected_prefixes\": [\"group_members_\"], \"expected_format\": \"csv\"},\n",
    "    \"f55_advisor\": {\"expected_prefixes\": [\"advisor_assignments_\", \"advisors_\"], \"expected_format\": \"csv\"},\n",
    "    \"my_cl_portal\": {\"expected_prefixes\": [\"claims_\"], \"expected_format\": \"csv\"},\n",
    "    \"climl_invest\": {\"expected_prefixes\": [\"investment_accounts_\"], \"expected_format\": \"csv\"},\n",
    "    \"group_retirement\": {\"expected_prefixes\": [\"group_retirement_plan_members_\", \"plan_members_\"], \"expected_format\": \"csv\"},\n",
    "    \"reinsurance\": {\"expected_prefixes\": [\"reinsurance_treaties_\"], \"expected_format\": \"csv\"},\n",
    "}\n",
    "\n",
    "TARGET_TABLES = {\n",
    "    \"salesforce_crm\": \"salesforce_crm_bronze\",\n",
    "    \"gwl_policy_admin\": \"gwl_policy_individual_life_bronze\",\n",
    "    \"ll_policy_admin\": \"ll_policy_individual_life_bronze\",\n",
    "    \"sap_billing\": \"sap_billing_invoices_bronze\",\n",
    "    \"adobe_analytics\": \"adobe_analytics_digital_events_bronze\",\n",
    "    \"avaya_call_centre\": \"call_centre_interactions_bronze\",\n",
    "    \"group_benefits\": \"group_benefits_plan_members_bronze\",\n",
    "    \"f55_advisor\": \"freedom55_advisor_assignments_bronze\",\n",
    "    \"my_cl_portal\": \"portal_digital_events_bronze\",\n",
    "    \"climl_invest\": \"climl_seg_fund_contracts_bronze\",\n",
    "    \"group_retirement\": \"group_retirement_plan_members_bronze\",\n",
    "    \"reinsurance\": \"reinsurance_treaty_data_bronze\",\n",
    "}\n"
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
     "nuid": "c52f7014-46eb-4cbf-8bc9-2a145707808c",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Runtime parameters"
    }
   },
   "outputs": [],
   "source": [
    "# This notebook is intended to run from a job.\n",
    "# Each parallel task passes the source-specific runtime values.\n",
    "\n",
    "SOURCE_NAME_ALIASES = {\n",
    "    \"salesforce.crm\": \"salesforce_crm\",\n",
    "    \"gwl_policy.individual_life\": \"gwl_policy_admin\",\n",
    "    \"ll_policy.individual_life\": \"ll_policy_admin\",\n",
    "    \"sap_billing.invoices\": \"sap_billing\",\n",
    "    \"adobe_analytics.digital_events\": \"adobe_analytics\",\n",
    "    \"call_centre.interactions\": \"avaya_call_centre\",\n",
    "    \"group_benefits.plan_members\": \"group_benefits\",\n",
    "    \"freedom55.advisor_assignments\": \"f55_advisor\",\n",
    "    \"portal.digital_events\": \"my_cl_portal\",\n",
    "    \"climl.seg_fund_contracts\": \"climl_invest\",\n",
    "    \"group_retirement.plan_members\": \"group_retirement\",\n",
    "    \"reinsurance.treaty_data\": \"reinsurance\",\n",
    "}\n",
    "\n",
    "dbutils.widgets.text(\"source_name\", \"\")\n",
    "dbutils.widgets.text(\"file_format\", \"csv\")\n",
    "dbutils.widgets.text(\"run_date\", \"\")\n",
    "\n",
    "source_name_input = dbutils.widgets.get(\"source_name\").strip()\n",
    "source_name = SOURCE_NAME_ALIASES.get(source_name_input, source_name_input)\n",
    "file_format = dbutils.widgets.get(\"file_format\").strip().lower()\n",
    "run_date_input = dbutils.widgets.get(\"run_date\").strip()\n",
    "\n",
    "if run_date_input:\n",
    "    try:\n",
    "        run_date = datetime.strptime(run_date_input, \"%Y-%m-%d\").strftime(\"%Y-%m-%d\")\n",
    "    except ValueError as error:\n",
    "        raise ValueError(\"run_date must use YYYY-MM-DD format when provided\") from error\n",
    "else:\n",
    "    run_date = date.today().strftime(\"%Y-%m-%d\")\n"
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
     "nuid": "d20c89fb-a73f-4b2b-8966-ba88577039aa",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Validate inputs"
    }
   },
   "outputs": [],
   "source": [
    "missing_required_parameters = [\n",
    "    parameter_name\n",
    "    for parameter_name, parameter_value in {\"source_name\": source_name_input}.items()\n",
    "    if not parameter_value\n",
    "]\n",
    "\n",
    "if missing_required_parameters:\n",
    "    print(\"Notebook is configured for job-driven parallel source ingestion.\")\n",
    "    print(\n",
    "        \"Set the required runtime parameters before running manually: \"\n",
    "        + \", \".join(missing_required_parameters)\n",
    "    )\n",
    "    dbutils.notebook.exit(\n",
    "        \"SKIPPED: missing required runtime parameters - \"\n",
    "        + \", \".join(missing_required_parameters)\n",
    "    )\n",
    "\n",
    "if file_format not in supported_formats:\n",
    "    raise ValueError(\n",
    "        f\"Unsupported file_format '{file_format}'. Use one of {sorted(supported_formats)}.\"\n",
    "    )\n",
    "\n",
    "if source_name not in SOURCE_CONTRACTS:\n",
    "    raise ValueError(\n",
    "        f\"Unsupported source_name '{source_name_input}'. Add it to SOURCE_CONTRACTS or SOURCE_NAME_ALIASES first.\"\n",
    "    )\n"
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
     "nuid": "cfcafded-72cc-4c76-bba7-5a97521f9f7d",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Resolve target table"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Resolved source_name  : salesforce_crm\nResolved run_date     : 2026-06-06\nResolved file_format  : csv\nResolved target table : dbw_c360_canadalife.bronze.salesforce_crm_bronze\n"
     ]
    }
   ],
   "source": [
    "# Resolve the bronze target table for the logical source.\n",
    "sanitized_source_name = re.sub(r\"[^a-zA-Z0-9_]\", \"_\", source_name.lower())\n",
    "table_name = TARGET_TABLES.get(source_name, f\"{sanitized_source_name}_bronze\")\n",
    "full_table_identifier = f\"{catalog}.{schema}.{table_name}\"\n",
    "\n",
    "print(f\"Requested source_name : {source_name_input}\")\n",
    "print(f\"Resolved source_name  : {source_name}\")\n",
    "print(f\"Resolved run_date     : {run_date}\")\n",
    "print(f\"Resolved file_format  : {file_format}\")\n",
    "print(f\"Resolved target table : {full_table_identifier}\")\n"
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
     "nuid": "98202b72-940a-46bd-8ae6-ee904c2e67f3",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Build source path"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Found data at: abfss://bronze@adlsc360canadalife.dfs.core.windows.net/salesforce_crm/year=2026/month=06/day=05/\nResolved source path : abfss://bronze@adlsc360canadalife.dfs.core.windows.net/salesforce_crm/year=2026/month=06/day=05/\n"
     ]
    }
   ],
   "source": [
    "from datetime import timedelta\n",
    "\n",
    "late_arrival_tolerance_days = 3\n",
    "base_path = f\"abfss://bronze@adlsc360canadalife.dfs.core.windows.net/{source_name}/\"\n",
    "parsed_run_date = datetime.strptime(run_date, \"%Y-%m-%d\")\n",
    "\n",
    "bronze_path = None\n",
    "for offset in range(late_arrival_tolerance_days + 1):\n",
    "    candidate_date = parsed_run_date - timedelta(days=offset)\n",
    "    candidate_path = (\n",
    "        f\"{base_path}year={candidate_date:%Y}/month={candidate_date:%m}/day={candidate_date:%d}/\"\n",
    "    )\n",
    "    try:\n",
    "        dbutils.fs.ls(candidate_path)\n",
    "        bronze_path = candidate_path\n",
    "        print(f\"Found data at: {bronze_path}\")\n",
    "        break\n",
    "    except Exception:\n",
    "        continue\n",
    "\n",
    "if bronze_path is None:\n",
    "    raise FileNotFoundError(\n",
    "        f\"Source path does not exist in ADLS — check that ADF has landed files for \"\n",
    "        f\"source_name='{source_name}' on run_date='{run_date}' (tolerance: {late_arrival_tolerance_days} days). \"\n",
    "        f\"Expected path: {base_path}\"\n",
    "    )\n",
    "\n",
    "print(f\"Resolved source path : {bronze_path}\")\n",
    "\n",
    "def collect_files_recursively(root_path: str):\n",
    "    files = []\n",
    "    stack = [root_path]\n",
    "    while stack:\n",
    "        current_path = stack.pop()\n",
    "        entries = dbutils.fs.ls(current_path)\n",
    "        files.extend(entry for entry in entries if entry.isFile())\n",
    "        stack.extend(entry.path for entry in entries if entry.isDir())\n",
    "    return files\n",
    "\n",
    "\n",
    "def detect_file_format_from_files(files):\n",
    "    for file_info in files:\n",
    "        file_name = file_info.name.lower()\n",
    "        if file_name.endswith(\".json\"):\n",
    "            return \"json\"\n",
    "        if file_name.endswith(\".csv\"):\n",
    "            return \"csv\"\n",
    "        if file_name.endswith(\".parquet\"):\n",
    "            return \"parquet\"\n",
    "    raise ValueError(\"Unable to determine file format from bronze files\")\n",
    "\n",
    "source_files = collect_files_recursively(bronze_path)\n",
    "if not source_files:\n",
    "    raise FileNotFoundError(f\"No files found under resolved bronze path: {bronze_path}\")\n",
    "\n",
    "expected_contract = SOURCE_CONTRACTS[source_name]\n",
    "expected_prefixes = expected_contract[\"expected_prefixes\"]\n",
    "expected_format = expected_contract[\"expected_format\"]\n",
    "source_file_names = [file_info.name for file_info in source_files]\n",
    "\n",
    "if not any(\n",
    "    any(file_name.startswith(expected_prefix) for expected_prefix in expected_prefixes)\n",
    "    for file_name in source_file_names\n",
    "):\n",
    "    raise ValueError(\n",
    "        f\"Resolved bronze path contains files that do not match the expected source contract for {source_name}. \"\n",
    "        f\"Expected one of {expected_prefixes}, found files: {source_file_names}\"\n",
    "    )\n",
    "\n",
    "effective_file_format = detect_file_format_from_files(source_files)\n",
    "if effective_file_format != expected_format:\n",
    "    raise ValueError(\n",
    "        f\"Resolved bronze path format mismatch for {source_name}. \"\n",
    "        f\"Expected format '{expected_format}', detected '{effective_file_format}'.\"\n",
    "    )\n",
    "\n",
    "print(f\"Expected filename prefixes: {expected_prefixes}\")\n",
    "print(f\"Detected file format      : {effective_file_format}\")"
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
     "nuid": "04ae9b0a-792c-4bee-bd4a-750f854bf1b3",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Validate source path exists"
    }
   },
   "outputs": [],
   "source": [
    "# Fail early if the expected landing path is missing in ADLS.\n",
    "try:\n",
    "    dbutils.fs.ls(bronze_path)\n",
    "except Exception:\n",
    "    raise FileNotFoundError(\n",
    "        f\"Source path does not exist in ADLS — check that ADF has landed files for \"\n",
    "        f\"source_name='{source_name}' on run_date='{run_date}'. \"\n",
    "        f\"Expected path: {bronze_path}\"\n",
    "    )\n"
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
     "nuid": "bc53fadf-64b7-4c66-893e-ff8276be2497",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Read bronze files"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Loaded source DataFrame for format: csv\n"
     ]
    }
   ],
   "source": [
    "reader = spark.read.format(effective_file_format)\n",
    "\n",
    "if effective_file_format == \"csv\":\n",
    "    reader = reader.option(\"header\", \"true\").option(\"inferSchema\", \"true\")\n",
    "elif effective_file_format == \"json\":\n",
    "    reader = reader.option(\"multiLine\", \"true\")\n",
    "\n",
    "df = reader.load(bronze_path)\n",
    "print(f\"Loaded source DataFrame for format: {effective_file_format}\")\n"
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
     "nuid": "7706832e-6bcc-4efc-9f99-82853f986be5",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Add bronze metadata columns"
    }
   },
   "outputs": [],
   "source": [
    "# Keep the bronze layer append-only and preserve file-level lineage.\n",
    "df_enriched = (\n",
    "    df.withColumn(\"source_system\", lit(source_name))\n",
    "    .withColumn(\"processing_date\", lit(run_date))\n",
    "    .withColumn(\"ingestion_timestamp\", current_timestamp())\n",
    "    .withColumn(\"source_file_path\", col(\"_metadata.file_path\"))\n",
    ")\n"
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
     "nuid": "8eb0586d-b518-407f-9f2f-d21f08881c66",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Create schema if needed"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Ensured schema exists: dbw_c360_canadalife.bronze\n"
     ]
    }
   ],
   "source": [
    "spark.sql(f\"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`\")\n",
    "print(f\"Ensured schema exists: {catalog}.{schema}\")\n"
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
     "nuid": "011bd8b8-90c0-4cc7-afce-91ef56f53528",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Append to Delta table"
    }
   },
   "outputs": [],
   "source": [
    "# Count before writing so the notebook can report the load size.\n",
    "record_count = df_enriched.count()\n",
    "write_mode = \"append\"\n",
    "action_message = \"Appended data to\"\n",
    "\n",
    "try:\n",
    "    spark.table(full_table_identifier).limit(1).collect()\n",
    "except Exception as error:\n",
    "    recoverable_errors = (\n",
    "        \"DELTA_TABLE_NOT_FOUND\",\n",
    "        \"DELTA_PATH_DOES_NOT_EXIST\",\n",
    "        \"TABLE_OR_VIEW_NOT_FOUND\",\n",
    "    )\n",
    "    if any(error_code in str(error) for error_code in recoverable_errors):\n",
    "        write_mode = \"overwrite\"\n",
    "        action_message = \"Recreated and loaded data into\"\n",
    "        print(f\"Target table is unavailable; recreating {full_table_identifier}\")\n",
    "    else:\n",
    "        raise\n",
    "\n",
    "writer = df_enriched.write.format(\"delta\").mode(write_mode)\n",
    "if write_mode == \"append\":\n",
    "    writer = writer.option(\"mergeSchema\", \"true\")\n",
    "else:\n",
    "    writer = writer.option(\"overwriteSchema\", \"true\")\n",
    "\n",
    "writer.saveAsTable(full_table_identifier)\n",
    "\n",
    "print(f\"Loaded {record_count} records from {bronze_path}\")\n",
    "print(f\"{action_message}: {full_table_identifier}\")\n",
    "dbutils.notebook.exit(\"SUCCESS\")\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": {
    "hardware": {
     "accelerator": null,
     "gpuPoolId": null,
     "memory": null
    },
    "software": {
     "pinSparkToX86": null
    }
   },
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
   "notebookName": "Bronze _Delta_tables",
   "widgets": {
    "file_format": {
     "currentValue": "csv",
     "nuid": "349cea08-1d16-4e71-bc85-0c26d1a75eb4",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "csv",
      "label": null,
      "name": "file_format",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "csv",
      "label": null,
      "name": "file_format",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "source_name": {
     "currentValue": "salesforce_crm",
     "nuid": "5d157e56-d613-4a56-b1f9-0f05dee1a527",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "",
      "label": null,
      "name": "source_name",
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
      "name": "source_name",
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
