import os
from typing import AsyncGenerator
from google.adk.events import Event, EventActions

from google.adk.agents import LlmAgent,  SequentialAgent, LoopAgent, BaseAgent
from google.adk.agents.invocation_context import InvocationContext

from billing_agent.validation_execution.tools import bigquery_validation


# Custom agent to check the status and escalate if 'pass'
class CheckStatusAndEscalate(BaseAgent):
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        should_stop = ctx.session.state.get("VALIDATION_EXIT", False)
        yield Event(author=self.name, actions=EventActions(escalate=should_stop))


# SQL Refiner agent that rewrites SQL based on error messages
sql_refiner_agent = LlmAgent(
    name="sql_refiner_agent",
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),
    instruction="""
You are a BigQuery SQL expert. Your task is to correct the input Bigquery SQL query based on the error message provided.

**Guidelines:**
 - ONLY OUTPUT SQL STATEMENT!!!
 - Read state['VALIDATION_ERROR'] and state['MODIFIED_SQL']
 - Refine {MODIFIED_SQL} according to the {VALIDATION_ERROR}
 - Output the modified SQL statement without any explanations or markdown formatting
 - If state['VALIDATION_ERROR'] is empty or does not exist, then just output {MODIFIED_SQL} as it is
""",
    output_key='MODIFIED_SQL'
)

# SQL Validator agent that checks if the SQL is valid using bigquery_validation
sql_validator_agent = LlmAgent(
    name="sql_validator_agent",
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),
    instruction="""
You are a BigQuery SQL validator. Your task is to check if the SQL statement is valid.

**Guidelines:**
 - retrieve the session state, and generate the final result in JSON format with four keys: "sql", "sql_error", "sql_query_results"
   "sql": {MODIFIED_SQL}
   "sql_error": {VALIDATION_ERROR}
   "sql_query_results": {QUERY_RESULTS}
""",
    before_agent_callback=bigquery_validation,
    output_key='VALIDATION_RESULT'  # Store validation result
)

# Custom agent to check the status and escalate if validation is successful


class CheckStatusAndEscalate(BaseAgent):
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        should_stop = ctx.session.state.get("VALIDATION_EXIT", False)
        print(f"CheckStatusAndEscalate: VALIDATION_EXIT = {should_stop}")

        # Always escalate if validation was successful
        if should_stop:
            print("SQL validation successful - stopping the loop")
            yield Event(
                author=self.name,
                actions=EventActions(escalate=True)
            )
        else:
            print("SQL validation failed - continuing the loop")
            yield Event(
                author=self.name,
                actions=EventActions(escalate=False)
            )


# BigQuery SQL Validator Loop Agent
refine_loop_agent = LoopAgent(
    name="RefinementLoop",
    sub_agents=[
        sql_validator_agent,  # First validate the SQL
        # Check if we should stop the loop
        CheckStatusAndEscalate(name="stop_checker"),
        sql_refiner_agent,    # Only refine if we didn't stop
    ],
    max_iterations=5,  # Limit loops to 5 iterations as required
)

report_composer = LlmAgent(
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),
    name="sql_validation_summary",
    include_contents="none",
    description="Transforms sql validation into a final answer.",
    instruction="""
    Transform the provided data into a polished, professional answer.

    ---
    ### the final result in JSON format including below
   "raw sql": {MODIFIED_SQL}
   "final sql": {MODIFIED_SQL}
   "final sql error": {VALIDATION_ERROR}
   "final sql results": {QUERY_RESULTS}

 
    """,
    output_key="FINAL_ANSWER",
)

refine_agent = SequentialAgent(
    name='refine_agent_pipeline',
    description="Executes a pre-defined refine pipeline. It performs sql validation, refine, and execution.",
    sub_agents=[
        refine_loop_agent,
        report_composer
    ]
)
# root_agent = LlmAgent(
#     name="BigQuerySqlExecutor",  # Added missing name parameter
#     model='gemini-2.5-pro',
#     sub_agents=[refine_agent],
#     instruction="""
# You are an intelligent BigQuery SQL statement executor. You can correct the user's input SQL, validate it, and return the query results if possible.

# Guidelines:
# 1. You must deligate the task to the `refine_agent` to verify, refine and execute the user input SQL.
# 2. DO not perform any sql statements by yourself, your job is to pass the task to `refine_agent` and summarize the appropreate response. 
# 3. Summarize the session state into final result in JSON format with four keys: "sql", "sql_error", "sql_query_results"
# """
# )
