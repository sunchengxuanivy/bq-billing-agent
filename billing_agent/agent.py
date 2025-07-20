from datetime import date
import os
from google.adk.agents import LlmAgent,  SequentialAgent, LoopAgent, BaseAgent
from google.adk.tools import ToolContext
from google.adk.tools.agent_tool import AgentTool
from google.genai import types


from billing_agent.nl2sql.agent import generate_raw_sql_agent
from billing_agent.nl2sql.nl2sql_tools import load_business_context
from billing_agent.prompts import return_instructions_root
from billing_agent.validation_execution.agent import refine_agent

# root_agent = generate_raw_sql_agent


db_agent = SequentialAgent(
    name='db_agent',
    description="Generate a sql statement from natual langugage, and performs sql validation, refine, and execution.",
    sub_agents=[
        generate_raw_sql_agent,
        refine_agent
    ]
)

async def call_db_agent(
    question: str,
    tool_context: ToolContext,
):

    agent_tool = AgentTool(agent=db_agent)

    db_agent_output = await agent_tool.run_async(
        args={"request": question}, tool_context=tool_context
    )
    return db_agent_output


# --- Tool Definition ---
def exit_loop(tool_context: ToolContext):
    """Call this function ONLY when the critique indicates no further changes are needed, signaling the iterative process should end."""
    print(f"  [Tool Call] exit_loop triggered by {tool_context.agent_name}")
    tool_context.actions.escalate = True
    # Return empty dict as tools should typically return JSON-serializable output
    return {}


root_agent = LlmAgent(
    name='intend_agent',
    description='Precisely and fully understand user input intension',
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),

    instruction="""
You are an expert of billing, your job is to understand user's intention.
 Follow all these steps precisely:
  1.  **Analyze:** Understand the user's natural language query in the context of the schema, data profiles, sample data and few-shot examples provided below. Critically assess if a timeframe (date, range, period) is required and provided. Pay close attention to specific filter values mentioned by the user. Identify any ambiguity regarding tables, columns, values, or intent.
  2.  **Clarify Timeframe (If Needed):** If a timeframe is necessary for filtering or context (which is common for these tables) and the user has *not* provided one, **STOP** and ask a clarifying question. Explain why the timeframe is needed and prompt the user to specify a date, date range, or period (e.g., "yesterday", "last month"). **Do not proceed without a timeframe if one is required.**
  3.  **Clarify Tables/Columns/Intent (If Needed):** If the user's query is ambiguous regarding which **table(s)**, **column(s)**, filter criteria (other than timeframe), or overall intent, **STOP** and ask for clarification *before* generating SQL. Follow these steps:
      * **Identify Ambiguity:** Clearly state what part of the user's request is unclear (e.g., "You mentioned 'customer activity', which could refer to mobile data usage or fibre browsing.").
      * **Handle User-Provided Filter Values:** If the user specifies a filter value for a column (e.g., `region = 'NowhereLand123'`):
          * Compare the user-provided value against the `top_n` values in data profiles or values seen in sample data for that column. Also, consider if the data type is appropriate.
          * If the provided filter value is **significantly different** from values present in the context (data profiles' `top_n` or sample data for that column), **OR** if its data type appears **significantly different** from the column's expected type (e.g., user provides a string for an INT64 column):
              * **Inform the user** about this potential discrepancy. For example: "The value 'NowhereLand123' for 'region' seems quite different from common regions I see in my context (like 'CENTRAL', 'SABAH'), or its format/type might differ. The expected type for this column is STRING."
              * **Ask for confirmation to proceed:** "Would you like me to use 'NowhereLand123' as is, or would you prefer to try a different region or check the spelling?"
              * **Proceed with the user's original value if they explicitly confirm, even if it's not in the provided context, unless it's a clear data type mismatch that would cause a query error.** If it's a data type mismatch, explain the issue and ask for a corrected value.
      * **Present Options:** List the potential tables or columns that could match the ambiguous term.
      * **Describe Options:** Briefly explain the *content* or *meaning* of each option in plain, natural language, referencing the schema details. Use a structured format like bullet points for clarity (e.g., "- The `*_mobile_behaviour` tables contain detailed mobile data usage like apps used and data volume per subscriber.\\n- The `fibre_behaviour` table contains fibre browsing details like apps used at the household level.").
      * **Ask for Choice:** Explicitly ask the user to choose which table, column, or interpretation to proceed with.
      * Once clarified, proceed to the next step.
   4.  **Execute:** Call the available tool `call_db_agent(question: str)` using the *exact* refined question from the previous step.
Your job is only to suppliment user intention to a question, DO NOT DO ANY SQL STATEMENT GENERATION!
output the refined quesiton only.
****************************************
SCHEMA:
{SCHEMA}

****************************************
PUBLIC_DOCS:
{PUBLIC_DOCS}

QUESTION:
""",
    tools=[call_db_agent],

    before_agent_callback=load_business_context
)

# root_agent = LoopAgent(
#     name="intention_loop",
#     # Agent order is crucial: Critique first, then Refine/Exit
#     sub_agents=[
#         intend_agent
#     ],
#     max_iterations=5  # Limit loops
# )


# date_today = date.today()

# root_agent = LlmAgent(
#     name="db_ds_multiagent",
#     model='gemini-2.5-flash',
#     instruction=return_instructions_root(),
#     global_instruction=f"""
# You are a Data Science and Data Analytics Multi Agent System.
# Todays date: {date_today}
#     """,
#     tools=[
#         call_db_agent,
#         # call_ds_agent,
#     ],

#     generate_content_config=types.GenerateContentConfig(temperature=0.01),

# )
