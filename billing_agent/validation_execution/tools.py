import datetime
import logging
import re
from google.adk.agents.invocation_context import InvocationContext
from google.cloud import bigquery
from billing_agent.nl2sql.nl2sql_tools import get_bq_client
from datetime import date, datetime
from decimal import Decimal
import json


def bigquery_validation(context: InvocationContext):
    """Validates the SQL using BigQuery and returns the results.
    Args:
        tool_context: The context for the tool execution.

    Returns:
        A dictionary containing the query result, status and an error message. 
    """
    logging.debug("Entering bigquery_validation")

    def json_serial(obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    def cleanup_sql(sql_string):
        """Processes the SQL string to get a printable, valid SQL string."""

        # 1. Remove backslashes escaping double quotes
        sql_string = sql_string.replace('\"', '"')

        # 2. Remove backslashes before newlines (the key fix for this issue)
        sql_string = sql_string.replace("\\n", "\n")  # Corrected regex

        # 3. Replace escaped single quotes
        sql_string = sql_string.replace("\'", "'")

        # 4. Replace escaped newlines (those not preceded by a backslash)
        sql_string = sql_string.replace("\n", "\n")

        return sql_string

    if not context.session.state.get('MODIFIED_SQL'):
        sql_string = context.session.state.get('FINAL_RAW_SQL')
        context.session.state.update({'MODIFIED_SQL': sql_string})
        context.session.state.update({'RAW_SQL': sql_string})
    else:
        sql_string = context.session.state.get('MODIFIED_SQL')

    sql_string = cleanup_sql(sql_string)
    sql_string = sql_string.replace("```sql", "").replace("```", "").strip()
    logging.debug("Validating SQL (after cleanup): %s", sql_string)

    final_result = {"query_result": None,
                    "query_status": None, "error_message": None}

    try:
        bq_client = get_bq_client()
        # Add labels to the job for tracking and cost analysis.
        # In a production environment, you might want to get the user from the
        # invocation context instead of an environment variable.

        job_config = bigquery.QueryJobConfig(
            labels={"source": "data-agent"}
        )
        query_job = bq_client.query(sql_string, job_config=job_config)
        logging.debug(f"Started BigQuery job: {query_job.job_id}")
        # Save the job ID to the session state.
        job_ids = context.session.state.get("JOB_IDS", [])
        if query_job.job_id not in job_ids:
            context.session.state.update(
                {"JOB_IDS": job_ids + [query_job.job_id]})
        # Get the query results, max 100 rows
        # This call blocks until the job is complete.
        results = query_job.result(max_results=100)

        # print(results.max_results)

        if results.schema:  # Check if query returned data
            rows = [
                {
                    key: value
                    for (key, value) in row.items()
                }
                for row in results
            ]
            # Convert rows to a JSON object
            json_object = json.dumps(rows, default=json_serial)
            # return f"Valid SQL. Results: {rows}"
            context.session.state.update({'VALIDATION_EXIT': True})
            context.session.state.update({'VALIDATION_ERROR': ""})
            context.session.state.update({'QUERY_RESULTS': json_object})

            final_result["query_status"] = "success"
            final_result["query_result"] = json_object

        else:
            context.session.state.update({'VALIDATION_EXIT': True})
            context.session.state.update({'VALIDATION_ERROR': ""})
            context.session.state.update({'QUERY_RESULTS': []})

            final_result["error_message"] = (
                "Valid SQL. Query executed successfully (no results)."
            )
            final_result["query_status"] = "success"

            context.session.state.update(
                {'VALIDATION_ERROR': final_result["error_message"]})

    except (
        Exception
    ) as e:  # Catch generic exceptions from BigQuery  # pylint: disable=broad-exception-caught
        final_result["error_message"] = f"Invalid SQL: {e}"
        final_result["query_status"] = "failed"
        context.session.state.update({'VALIDATION_EXIT': False})
        context.session.state.update(
            {'VALIDATION_ERROR': final_result["error_message"]})
        context.session.state.update({'QUERY_RESULTS': []})

    logging.debug(f"Validation Result: {final_result}")

    print(f"Validating SQL (after cleanup): {sql_string}")
    print(f"Validation Result: {final_result}")

    return final_result
