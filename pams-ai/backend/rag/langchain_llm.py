from __future__ import annotations

import os
from typing import Optional

from langchain_community.chat_models import ChatOllama
from langchain_core.language_models.chat_models import BaseChatModel


DEFAULT_MODEL = os.getenv("LLM_MODEL", "llama3.2")
DEFAULT_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))
DEFAULT_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "60"))


class LLMUnavailable(Exception):
    """Erreur explicite quand le LLM n'est pas accessible."""


def get_llm(
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    timeout: Optional[int] = None,
) -> BaseChatModel:
    """
    Factory unique pour le LLM.
    - Centralise les paramètres
    - Limite les hallucinations
    - Prête pour MCP / policy
    """

    model = model or DEFAULT_MODEL
    temperature = temperature if temperature is not None else DEFAULT_TEMPERATURE
    timeout = timeout or DEFAULT_TIMEOUT

    try:
        llm = ChatOllama(
            model=model,
            temperature=temperature,
            request_timeout=timeout,
        )

        # test

        return llm

    except Exception as e:
        raise LLMUnavailable(
            f"LLM Ollama indisponible (model={model}): {e}"
        ) from e
