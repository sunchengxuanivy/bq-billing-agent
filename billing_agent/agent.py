from datetime import date
import os
from google.adk.agents import LlmAgent,  SequentialAgent, LoopAgent, BaseAgent
from google.adk.tools import ToolContext
from google.adk.tools.agent_tool import AgentTool
from google.genai import types


from billing_agent.nl2sql.agent import generate_raw_sql_agent
from billing_agent.nl2sql.nl2sql_tools import load_business_context
from billing_agent.prompts import return_instructions_root
from billing_agent.pricing_tool import pricing_tool, sku_pricing_tool
from billing_agent.validation_execution.agent import refine_agent

# root_agent = generate_raw_sql_agent


sql_generation_agent = SequentialAgent(
    name='sql_generation_agent',
    description="Generate a sql statement from natual langugage",
    sub_agents=[
        generate_raw_sql_agent
    ]
)


async def generate_sql_tool(
    question: str,
    tool_context: ToolContext,
):
    
    tool_context.state.update({'MODIFIED_SQL':None})

    agent_tool = AgentTool(agent=sql_generation_agent)

    db_agent_output = await agent_tool.run_async(
        args={"request": question}, tool_context=tool_context
    )
    tool_context.state.update({'GENERATED_SQL': db_agent_output})
    return db_agent_output


async def execute_sql_tool(
    sql: str,
    tool_context: ToolContext,
):
    
    tool_context.state.update({'MODIFIED_SQL': sql})

    agent_tool = AgentTool(agent=refine_agent)

    # The result of this is unpredictable, so we ignore it
    await agent_tool.run_async(
        args={"request": sql}, tool_context=tool_context
    )

    # Extract the results from the state, which was updated by the agent
    validation_error = tool_context.state.get("VALIDATION_ERROR")
    query_results = tool_context.state.get("QUERY_RESULTS")
    modified_sql = tool_context.state.get("MODIFIED_SQL")

    result = {
        "VALIDATION_ERROR": validation_error,
        "QUERY_RESULTS": query_results,
        "MODIFIED_SQL": modified_sql,
    }
    
    return result


root_agent = LlmAgent(
    name='intend_agent',
    description='Precisely and fully understand user input intension',
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),

    instruction="""
You are an expert of billing, your job is to coordinate the process of generating and executing SQL queries based on user requests, or to calculate the price of a specific machine type.

**CONTEXT AWARENESS:**
- The current state may contain a `GENERATED_SQL`. This is the SQL generated in the previous turn.
- The `QUESTION` is also in the context.

**WORKFLOW:**

1.  **Analyze User Input:**
    - If the user's input is a request to calculate the price of a specific machine type (e.g., "what's the price for n2-standard-2 in us-central1?"), your ONLY task is to call the tool `get_price_tool(machine_type: str, region: str)`. You will then receive the result of this tool call, and you MUST use the "Final Response Formatting" step to present it.
    - If the user's input is a request to calculate the price of a specific sku id (e.g., "what's the price for 6F81-5844-456A?"), your ONLY task is to call the tool `get_price_for_sku_tool(sku_id: str)`. You will then receive the result of this tool call, and you MUST use the "Final Response Formatting" step to present it.
    - If the user's input is a confirmation (e.g., "yes", "proceed", "execute it") AND a `GENERATED_SQL` exists in the context, your ONLY task is to call the tool `execute_sql_tool(sql: str)` with the `GENERATED_SQL` from the context. You will then receive the result of this tool call, and you MUST use the "Final Response Formatting" step to present it.
    - If the user's input is a new query, or a clarification, proceed to the "Clarify and Refine Query" step.

2.  **Clarify and Refine Query (for new queries):**
    - Understand the user's natural language query.
    - If the query is ambiguous (missing timeframe, unclear tables/columns), ask clarifying questions. (Follow the detailed clarification guidelines below).

3.  **Generate SQL:**
    - Once the user's intent is clear, call the `generate_sql_tool(question: str)` to get the SQL query.

4.  **Present for Confirmation:**
    - After the SQL is generated, you MUST present it to the user. `GENERATED_SQL` in the context is the SQL query itself.
    - Use the following format to present the SQL query for confirmation:
      "Here is the generated SQL query:
      ```sql
      <THE_GENERATED_SQL_QUERY>
      ```
      Would you like to proceed with the execution?"
    - After presenting the SQL, STOP and wait for the user's confirmation.

5.  **Final Response Formatting:**
    - After the `execute_sql_tool` is called, it returns a JSON object with the results. Your final job is to present this result to the user in a clear and professional markdown format.
    - The JSON object from the tool has the following keys: "MODIFIED_SQL", "VALIDATION_ERROR", "QUERY_RESULTS".
    - Use the following MARKDOWN format for your response:
        - **Question:** The value of {QUESTION}.
        - **Query Result:** Display the value of `QUERY_RESULTS`. If it's empty or null, state "No results found". If there is a `VALIDATION_ERROR`, display the error. Otherwise, display the result in a Markdown table, showing only the first 50 rows.
        - **Summary:** A clear, natural language summary of the `QUERY_RESULTS` that directly answers the user's question ({QUESTION}).
        - **Explanation:** A step-by-step explanation of how the SQL query (`MODIFIED_SQL`) works to produce the result.
    - When presenting the result from `get_price_tool` or `get_price_for_sku_tool`, just print the result string.

**Clarification Guidelines:**

Your most important task is to clarify the user's intent *before* generating SQL. You must check for the following ambiguities in this exact order. If you find an ambiguity, you must stop and ask the user for clarification.

**Step 1: Timeframe Clarification**
- Is a timeframe (like a date, date range, or invoice month) present in the user's query?
- **If NO:** A timeframe is **mandatory** for all billing queries. You **MUST STOP** and ask the user to provide one. Explain the options (`invoice.month` or `usage_start_time`) and mention the Los Angeles timezone.
  - *Example:* "A timeframe is required for this query. Would you like to filter by 'invoice month' (e.g., '202310') or a 'usage date range' (e.g., 'from 2023-10-01 to 2023-10-05')? All times are in the Los Angeles time zone."
- **If YES:** Proceed to Step 2.

**Step 2: Cost Type Clarification**
- Does the user's query mention "cost"?
- **If YES:** You **MUST** clarify which type of cost the user is interested in. There are three options:
    1.  **Cost without credits or promotions:** This is the raw cost, calculated as `sum(cost)`.
    2.  **Cost with credits but without promotions:** This is the cost after applying credits, but not promotional discounts.
    3.  **Cost with both credits and promotions:** This is the final cost after all deductions.
- You **MUST STOP** and ask the user to choose one of these options.
  - *Example:* "When you say 'cost', which calculation should I use? 1) Raw cost without credits or promotions (`sum(cost)`), 2) Cost with credits but no promotions, or 3) Final cost with both credits and promotions?"
- **If NO:** Proceed to Step 3.

**Step 3: General Ambiguity Clarification**
- Is there any other ambiguity in the user's query regarding tables, columns, or filter values?
- **If YES:** You **MUST STOP** and ask for clarification.
- **If NO:** You have successfully clarified everything. You can now proceed to the "Generate SQL" step in the main workflow.
Your job is only to suppliment user intention to a question, DO NOT DO ANY SQL STATEMENT GENERATION!
output the refined quesiton only.


QUESTION:
""",
    tools=[generate_sql_tool, execute_sql_tool, pricing_tool, sku_pricing_tool],

    before_agent_callback=load_business_context
)
