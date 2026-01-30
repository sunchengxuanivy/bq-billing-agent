import logging
import os
from google.adk.agents.callback_context import CallbackContext
import requests
from google.cloud import bigquery

# Initialize global variables
bq_client = None


def fetch_web_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.text
    except Exception as e:
        print(f"Error fetching content from {url}: {e}")
        return None


def get_bq_client():
    """
    Get or create a BigQuery client.

    Returns:
        bigquery.Client: A BigQuery client instance.

    Raises:
        ValueError: If BQ_PROJECT_ID environment variable is not set.
    """
    global bq_client

    project_id = os.getenv("BQ_PROJECT_ID")
    if project_id is None:
        error_msg = "BQ_PROJECT_ID environment variable is not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    if bq_client is None:
        project_id = project_id.strip("'").strip('"').strip()
        logging.info(
            f"Initializing BigQuery client with project ID: {project_id}")
        bq_client = bigquery.Client(project=project_id)
    return bq_client


def load_table_schema(callback_context: CallbackContext) -> None:

    table_id = os.getenv('PROTOTYPE_DETAILED_BILLING_TABLE_ID')
    if table_id is None:
        error_msg = "PROTOTYPE_DETAILED_BILLING_TABLE_ID environment variable is not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    callback_context.state.update(
        {'PROTOTYPE_DETAILED_BILLING_TABLE_ID': table_id})

    if callback_context.state.get('SCHEMA') is None:

        bigquery_client = get_bq_client()
        table_obj = bigquery_client.get_table(table_id)

        ddl_statement = f"CREATE OR REPLACE TABLE `{table_id}` (\n"

        for field in table_obj.schema:
            ddl_statement += f"  `{field.name}` {field.field_type}"
            if field.mode == "REPEATED":
                ddl_statement += " ARRAY"
            if field.description:
                ddl_statement += f" COMMENT '{field.description}'"
            ddl_statement += ",\n"

        ddl_statement = ddl_statement[:-2] + "\n);\n\n"

        # Add example values if available (limited to first row)
        rows = bigquery_client.list_rows(
            table_id, max_results=5).to_dataframe()
        if not rows.empty:
            ddl_statement += f"-- Example values for table `{table_id}`:\n"
            for _, row in rows.iterrows():  # Iterate over DataFrame rows
                ddl_statement += f"INSERT INTO `{table_id}` VALUES\n"
                example_row_str = "("
                for value in row.values:  # Now row is a pandas Series and has values
                    if isinstance(value, str):
                        example_row_str += f"'{value}',"
                    elif value is None:
                        example_row_str += "NULL,"
                    else:
                        example_row_str += f"{value},"
                example_row_str = (
                    example_row_str[:-1] + ");\n\n"
                )  # remove trailing comma
                ddl_statement += example_row_str

        callback_context.state.update({'SCHEMA': ddl_statement})


def load_business_context(callback_context: CallbackContext) -> None:

    load_table_schema(callback_context)

    if not callback_context.state.get('PUBLIC_DOCS'):

        table_explanation = fetch_web_content(
            "https://cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/detailed-usage")

        bq_examples = fetch_web_content(
            "https://cloud.google.com/billing/docs/how-to/bq-examples")
        callback_context.state.update(
            {'PUBLIC_DOCS': f"{table_explanation}\n\n{bq_examples}"})

    # # TODO: treat raw client input as the question. text only.
    callback_context.state.update(
        {'QUESTION': callback_context._invocation_context.user_content.parts[0].text})
    # print(callback_context.state.to_dict())


def load_target_billing_context(callback_context: CallbackContext) -> None:
    target_billing_tables = os.getenv('TARGET_BILLING_TABLES')
    if target_billing_tables is None:
        error_msg = "TARGET_BILLING_TABLES environment variable is not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    callback_context.state.update(
        {'TARGET_BILLING_TABLES': target_billing_tables.split(',')})


def load_nl2sql_expand_context(callback_context: CallbackContext) -> None:
    load_business_context(callback_context)
    load_target_billing_context(callback_context)
