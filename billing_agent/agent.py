from datetime import date
from google.adk.agents import LlmAgent,  SequentialAgent, LoopAgent, BaseAgent
from google.adk.tools import ToolContext
from google.adk.tools.agent_tool import AgentTool
from google.genai import types


from billing_agent.nl2sql.agent import generate_raw_sql_agent
from billing_agent.prompts import return_instructions_root
from billing_agent.validation_execution.agent import refine_agent

# root_agent = generate_raw_sql_agent


root_agent = SequentialAgent(
    name='db_agent',
    description="Generate a sql statement from natual langugage, and performs sql validation, refine, and execution.",
    sub_agents=[
        generate_raw_sql_agent,
        refine_agent
    ]
)


# async def call_db_agent(
#     question: str,
#     tool_context: ToolContext,
# ):

#     agent_tool = AgentTool(agent=db_agent)

#     db_agent_output = await agent_tool.run_async(
#         args={"request": question}, tool_context=tool_context
#     )
#     return db_agent_output

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
