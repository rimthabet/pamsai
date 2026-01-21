# agent/lc_agent.py
from typing import Any, Dict
from langchain_community.chat_models import ChatOllama

from agent.prompts import AGENT_PROMPT
from tools.langchain_tools import (
    set_current_role,
    tool_get_project_by_name,
    tool_get_fund_by_name,
    tool_list_funds,
    tool_create_project,
)

TOOLS = [
    tool_get_project_by_name,
    tool_get_fund_by_name,
    tool_list_funds,
    tool_create_project,
]

def run_agent(question: str, model: str = "llama3.2", role: str = "viewer") -> Dict[str, Any]:
    """
    Exécute un agent tool-calling.
    Retourne un dict: {text, raw}
    """
    set_current_role(role)

    llm = ChatOllama(model=model, temperature=0.1)
    llm = llm.bind_tools(TOOLS)

    chain = AGENT_PROMPT | llm
    msg = chain.invoke({"question": question})

    # msg.content = réponse textuelle finale
    # msg.tool_calls éventuels selon version LC / Ollama
    return {
        "text": (getattr(msg, "content", "") or "").strip(),
        "raw": msg,
    }
