import os

from google.adk.agents import LlmAgent, SequentialAgent
from google.genai import types

from billing_agent.prompts import NL2SQL_PROMPT, EXPAND_PROMPT
from billing_agent.nl2sql.nl2sql_tools import load_business_context, load_target_billing_context


nl2sql_agent = LlmAgent(
    name="nl2sql_agent",
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),
    instruction=NL2SQL_PROMPT,
    output_key='PROTOTYPE_SQL',
    generate_content_config=types.GenerateContentConfig(temperature=0.01),
)


expand_agent = LlmAgent(
    name="expand_agent",
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),
    instruction=EXPAND_PROMPT,
    output_key='FINAL_RAW_SQL',
    before_agent_callback=load_target_billing_context,
    generate_content_config=types.GenerateContentConfig(temperature=0.01),
)

generate_raw_sql_agent = SequentialAgent(
    name="generate_raw_sql_agent",
    # Run parallel research first, then merge
    sub_agents=[nl2sql_agent, expand_agent],
    description="Coordinates sequential nl2sql and expand_agent to get the right SQL statement.",
)
