import os

from google.adk.agents import LlmAgent
from google.genai import types

from billing_agent.prompts import NL2SQL_EXPAND_PROMPT
from billing_agent.nl2sql.nl2sql_tools import load_nl2sql_expand_context


generate_raw_sql_agent = LlmAgent(
    name="generate_raw_sql_agent",
    model=os.getenv('AGENT_MODEL', 'gemini-2.5-flash'),
    instruction=NL2SQL_EXPAND_PROMPT,
    output_key='FINAL_RAW_SQL',
    before_agent_callback=load_nl2sql_expand_context,
    generate_content_config=types.GenerateContentConfig(temperature=0.01),
)
