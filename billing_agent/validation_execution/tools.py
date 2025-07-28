import datetime
import logging
import re
from google.adk.agents.callback_context import CallbackContext
from google.cloud import bigquery
from billing_agent.nl2sql.nl2sql_tools import get_bq_client


def bigquery_validation(callback_context: CallbackContext
                        ):
    def cleanup_sql(sql_string):
        """Processes the SQL string to get a printable, valid SQL string."""

        # 1. Remove backslashes escaping double quotes
        sql_string = sql_string.replace('\\"', '"')

        # 2. Remove backslashes before newlines (the key fix for this issue)
        sql_string = sql_string.replace("\\\n", "\n")  # Corrected regex

        # 3. Replace escaped single quotes
        sql_string = sql_string.replace("\\'", "'")

        # 4. Replace escaped newlines (those not preceded by a backslash)
        sql_string = sql_string.replace("\\n", "\n")

        return sql_string

    if not callback_context.state.get('MODIFIED_SQL'):
        sql_string = callback_context.state.get('FINAL_RAW_SQL')
        callback_context.state.update({'MODIFIED_SQL': sql_string})
        callback_context.state.update({'RAW_SQL': sql_string})
    else:
        sql_string = callback_context.state.get('MODIFIED_SQL')

    sql_string = cleanup_sql(sql_string)
    sql_string = sql_string.replace("```sql", "").replace("```", "").strip()
    logging.info("Validating SQL (after cleanup): %s", sql_string)

    final_result = {"query_result": None, "error_message": None}

    # More restrictive check for BigQuery - disallow DML and DDL
    if re.search(
        r"(update|delete|drop|insert|create|alter|truncate|merge)", sql_string, re.IGNORECASE
    ):
        final_result["error_message"] = (
            "Invalid SQL: Contains disallowed DML/DDL operations."
        )
        callback_context.state.update({'VALIDATION_EXIT': True})
        callback_context.state.update(
            {'VALIDATION_ERROR': final_result["error_message"]})
        callback_context.state.update({'QUERY_RESULTS': []})

        return

    try:
        bq_client = get_bq_client()
        query_job = bq_client.query(sql_string)
        results = query_job.result()  # Get the query results

        print(results.max_results)
        if results.schema:  # Check if query returned data
            rows = [
                {
                    key: (
                        value
                        if not isinstance(value, datetime.date)
                        else value.strftime("%Y-%m-%d")
                    )
                    for (key, value) in row.items()
                }
                for row in results
            ]
            # return f"Valid SQL. Results: {rows}"
            callback_context.state.update({'VALIDATION_EXIT': True})
            callback_context.state.update({'VALIDATION_ERROR': ""})
            callback_context.state.update({'QUERY_RESULTS': rows})

        else:
            callback_context.state.update({'VALIDATION_EXIT': True})
            callback_context.state.update({'VALIDATION_ERROR': ""})
            callback_context.state.update({'QUERY_RESULTS': []})

            final_result["error_message"] = (
                "Valid SQL. Query executed successfully (no results)."
            )
            callback_context.state.update(
                {'VALIDATION_ERROR': final_result["error_message"]})

    except (
        Exception
    ) as e:  # Catch generic exceptions from BigQuery  # pylint: disable=broad-exception-caught
        final_result["error_message"] = f"Invalid SQL: {e}"
        callback_context.state.update({'VALIDATION_EXIT': False})
        callback_context.state.update(
            {'VALIDATION_ERROR': final_result["error_message"]})
        callback_context.state.update({'QUERY_RESULTS': []})
    return
