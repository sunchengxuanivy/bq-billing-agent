from google.adk.agents import LlmAgent,  SequentialAgent, LoopAgent, BaseAgent


from billing_agent.nl2sql.agent import generate_raw_sql_agent
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
