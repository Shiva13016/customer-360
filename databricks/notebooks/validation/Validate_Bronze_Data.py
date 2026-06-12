# Databricks notebook source
# MAGIC %md
# MAGIC # Validate_Bronze_Data
# MAGIC
# MAGIC **Layer:** Validation
# MAGIC **Purpose:** Validates raw source data before loading into Bronze Delta tables.
# MAGIC Checks for schema conformance, null rates, row counts, and source-specific business rules.
# MAGIC
# MAGIC **Inputs:** Raw source data from ADLS raw-landing zone
# MAGIC **Outputs:** Validation pass/fail status written to control table
# MAGIC
# MAGIC **Usage in DAB job:** Called per source via `base_parameters: { source: <source_name> }`
# MAGIC
# MAGIC **Source notebook path (workspace):**
# MAGIC `/Users/shivakumaryallanti5@gmail.com/project customer 360/Validate_Bronze_Data`

# COMMAND ----------

# Parameters - passed via DAB job base_parameters
dbutils.widgets.text("source", "", "Source Name")
source = dbutils.widgets.get("source")
print(f"Validating source: {source}")

# COMMAND ----------

{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ffe95ac1-098f-45c2-b7cc-0d8b24e42626",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Notebook overview"
    }
   },
   "source": [
    "This notebook validates bronze-layer file delivery before downstream processing starts.\n",
    "\n",
    "It is designed to act as a hard gate:\n",
    "* Requires only `source_name`\n",
    "* Uses the system date at runtime\n",
    "* Supports a 3-day late-arrival tolerance window\n",
    "* Accepts an optional `run_date` override for deterministic backfills and replays\n",
    "* Fails the notebook task when no valid files are found or when delivered files are empty\n",
    "\n",
    "Execution flow:\n",
    "* Setup parameters and validation window\n",
    "* Define reusable helper functions\n",
    "* Validate the delivery folder and compute quality checks\n",
    "* Return success details or fail the task"
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
     "nuid": "99340c83-7c15-41e4-830b-e1d28edc89de",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Setup parameters"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Requested source name: call_centre.interactions\nResolved source name : avaya_call_centre\nBase path            : abfss://bronze@adlsc360canadalife.dfs.core.windows.net/avaya_call_centre/\nEffective run date   : 2026/06/08\nAccepted delivery dates: 2026/06/08, 2026/06/07, 2026/06/06, 2026/06/05\n"
     ]
    }
   ],
   "source": [
    "from datetime import datetime, date, timedelta\n",
    "\n",
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
    "# Required input\n",
    "dbutils.widgets.text(\"source_name\", \"\")\n",
    "dbutils.widgets.text(\"run_date\", \"\")\n",
    "\n",
    "source_name_input = dbutils.widgets.get(\"source_name\").strip()\n",
    "run_date_input = dbutils.widgets.get(\"run_date\").strip()\n",
    "source_name = SOURCE_NAME_ALIASES.get(source_name_input, source_name_input)\n",
    "\n",
    "if not source_name_input:\n",
    "    raise ValueError(\"source_name parameter is required\")\n",
    "if source_name not in SOURCE_CONTRACTS:\n",
    "    raise ValueError(\n",
    "        f\"Unsupported source_name '{source_name_input}'. Add it to SOURCE_CONTRACTS or SOURCE_NAME_ALIASES first.\"\n",
    "    )\n",
    "\n",
    "if run_date_input:\n",
    "    try:\n",
    "        system_date = datetime.strptime(run_date_input, \"%Y-%m-%d\").date()\n",
    "    except ValueError as error:\n",
    "        raise ValueError(\"run_date must use YYYY-MM-DD format when provided\") from error\n",
    "else:\n",
    "    system_date = date.today()\n",
    "\n",
    "late_arrival_tolerance_days = 3\n",
    "validation_dates = [\n",
    "    system_date - timedelta(days=offset)\n",
    "    for offset in range(late_arrival_tolerance_days + 1)\n",
    "]\n",
    "\n",
    "base_path = f\"abfss://bronze@adlsc360canadalife.dfs.core.windows.net/{source_name}/\"\n",
    "\n",
    "print(f\"Requested source name: {source_name_input}\")\n",
    "print(f\"Resolved source name : {source_name}\")\n",
    "print(f\"Base path            : {base_path}\")\n",
    "print(f\"Effective run date   : {system_date.strftime('%Y/%m/%d')}\")\n",
    "print(\n",
    "    \"Accepted delivery dates: \"\n",
    "    + \", \".join(validation_date.strftime('%Y/%m/%d') for validation_date in validation_dates)\n",
    ")"
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
     "nuid": "6dfc3058-2f40-4d00-9b94-8214fd9f265e",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Helper functions"
    }
   },
   "outputs": [],
   "source": [
    "def detect_file_format(files):\n",
    "    for file_info in files:\n",
    "        file_name = file_info.name.lower()\n",
    "        if file_name.endswith(\".json\"):\n",
    "            return \"json\"\n",
    "        if file_name.endswith(\".csv\"):\n",
    "            return \"csv\"\n",
    "        if file_name.endswith(\".parquet\"):\n",
    "            return \"parquet\"\n",
    "    raise ValueError(\"Unable to determine file format from delivered files\")\n",
    "\n",
    "\n",
    "def read_files_for_format(file_format: str, file_paths):\n",
    "    if file_format == \"json\":\n",
    "        return spark.read.option(\"multiLine\", \"true\").json(file_paths)\n",
    "    if file_format == \"parquet\":\n",
    "        return spark.read.parquet(*file_paths)\n",
    "    return (\n",
    "        spark.read.option(\"header\", \"true\")\n",
    "        .option(\"inferSchema\", \"true\")\n",
    "        .csv(file_paths)\n",
    "    )\n",
    "\n",
    "\n",
    "def build_candidate_delivery_paths(root_path: str, delivery_date: date):\n",
    "    year_part = f\"year={delivery_date.year}\"\n",
    "    month_parts = [f\"month={delivery_date.month:02d}\", f\"month={delivery_date.month}\"]\n",
    "    day_parts = [f\"day={delivery_date.day:02d}\", f\"day={delivery_date.day}\"]\n",
    "\n",
    "    candidates = []\n",
    "    for month_part in month_parts:\n",
    "        for day_part in day_parts:\n",
    "            candidate = f\"{root_path}{year_part}/{month_part}/{day_part}/\"\n",
    "            if candidate not in candidates:\n",
    "                candidates.append(candidate)\n",
    "    return candidates\n",
    "\n",
    "\n",
    "def collect_files_recursively(root_path: str):\n",
    "    files = []\n",
    "    stack = [root_path]\n",
    "\n",
    "    while stack:\n",
    "        current_path = stack.pop()\n",
    "        entries = dbutils.fs.ls(current_path)\n",
    "        files.extend(entry for entry in entries if entry.isFile())\n",
    "        stack.extend(entry.path for entry in entries if entry.isDir())\n",
    "\n",
    "    return files\n",
    "\n",
    "\n",
    "def find_delivery_path(root_path: str, delivery_dates):\n",
    "    checked_paths = []\n",
    "\n",
    "    for delivery_date in delivery_dates:\n",
    "        candidate_paths = build_candidate_delivery_paths(root_path, delivery_date)\n",
    "        for candidate_path in candidate_paths:\n",
    "            checked_paths.append(candidate_path)\n",
    "            try:\n",
    "                files = collect_files_recursively(candidate_path)\n",
    "                if files:\n",
    "                    return delivery_date, candidate_path, files, checked_paths\n",
    "            except Exception:\n",
    "                continue\n",
    "\n",
    "    checked_dates = \", \".join(\n",
    "        delivery_date.strftime('%Y/%m/%d') for delivery_date in delivery_dates\n",
    "    )\n",
    "    raise FileNotFoundError(\n",
    "        f\"No files found for delivery dates [{checked_dates}] under {root_path}. \"\n",
    "        f\"Checked paths: {', '.join(checked_paths)}\"\n",
    "    )"
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
     "nuid": "a08e56f9-40ef-4c94-8ec6-9f96382a190f",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "Run bronze validation"
    }
   },
   "outputs": [],
   "source": [
    "print(f\"--- Starting Validation for {source_name} ---\")\n",
    "\n",
    "try:\n",
    "    matched_date, delivery_path, delivered_files, checked_paths = find_delivery_path(\n",
    "        base_path, validation_dates\n",
    "    )\n",
    "    file_format = detect_file_format(delivered_files)\n",
    "    expected_contract = SOURCE_CONTRACTS[source_name]\n",
    "    expected_prefixes = expected_contract[\"expected_prefixes\"]\n",
    "    expected_format = expected_contract[\"expected_format\"]\n",
    "    delivered_file_names = [file_info.name for file_info in delivered_files]\n",
    "\n",
    "    if not any(\n",
    "        any(file_name.startswith(expected_prefix) for expected_prefix in expected_prefixes)\n",
    "        for file_name in delivered_file_names\n",
    "    ):\n",
    "        raise ValueError(\n",
    "            f\"Validation FAILED: expected one of filename prefixes {expected_prefixes} for {source_name}, \"\n",
    "            f\"but found files: {delivered_file_names}\"\n",
    "        )\n",
    "    if file_format != expected_format:\n",
    "        raise ValueError(\n",
    "            f\"Validation FAILED: expected file format '{expected_format}' for {source_name}, detected '{file_format}'\"\n",
    "        )\n",
    "\n",
    "    file_paths = [file_info.path for file_info in delivered_files]\n",
    "    df = read_files_for_format(file_format, file_paths)\n",
    "\n",
    "    row_count = df.count()\n",
    "    file_count = len(delivered_files)\n",
    "\n",
    "    latest_file = max(\n",
    "        delivered_files,\n",
    "        key=lambda file_info: getattr(file_info, \"modificationTime\", 0),\n",
    "    )\n",
    "    latest_file_name = latest_file.name\n",
    "    latest_file_path = latest_file.path\n",
    "    latest_file_row_count = read_files_for_format(file_format, [latest_file_path]).count()\n",
    "\n",
    "    print(f\"Checked delivery paths: {checked_paths}\")\n",
    "    print(f\"Matched delivery date: {matched_date.strftime('%Y/%m/%d')}\")\n",
    "    print(f\"Matched delivery path: {delivery_path}\")\n",
    "    print(f\"Detected format: {file_format}\")\n",
    "    print(f\"Expected filename prefixes: {expected_prefixes}\")\n",
    "    print(f\"File count: {file_count}\")\n",
    "    print(f\"Total row count: {row_count}\")\n",
    "    print(f\"Latest file name: {latest_file_name}\")\n",
    "    print(f\"Latest file row count: {latest_file_row_count}\")\n",
    "\n",
    "    if row_count == 0:\n",
    "        raise ValueError(\n",
    "            \"Validation FAILED: Delivered files exist for the accepted delivery window but contain 0 rows.\"\n",
    "        )\n",
    "\n",
    "    result_msg = (\n",
    "        f\"SUCCESS|{matched_date.strftime('%Y/%m/%d')}|{row_count}|{file_count}|\"\n",
    "        f\"{latest_file_name}|{file_format}|{latest_file_row_count}\"\n",
    "    )\n",
    "    print(result_msg)\n",
    "    dbutils.notebook.exit(result_msg)\n",
    "\n",
    "except Exception as e:\n",
    "    result_msg = f\"FAILURE|{str(e)}\"\n",
    "    print(result_msg)\n",
    "    raise RuntimeError(result_msg) from e"
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
   "notebookName": "Validate_Bronze_Data",
   "widgets": {
    "run_date": {
     "currentValue": "2026-06-08",
     "nuid": "566c1a61-4148-42ba-97b6-b17678ef35d9",
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
    "source_name": {
     "currentValue": "call_centre.interactions",
     "nuid": "319441e2-e389-4c7f-9f5a-f4ab5ae22588",
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
