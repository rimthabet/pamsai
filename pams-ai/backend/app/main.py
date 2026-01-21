from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List

from app.chat_service import chat_pipeline

app = FastAPI(title="PAMS-AI Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    top_k: int = 8
    model: str = "llama3.2"
    role: str = "viewer"
    debug: bool = False
    mode: str = "rag"  


class ChatResponse(BaseModel):
    answer: str
    sources: List[Dict[str, Any]] = []
    suggested_actions: List[Dict[str, Any]] = []
    navigation: List[Dict[str, Any]] = []
    used: Dict[str, Any] = {}


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    try:
        out = chat_pipeline(
            message=req.message,
            top_k=req.top_k,
            model=req.model,
            role=req.role,
            debug=req.debug,
            mode=req.mode,
        )
        # stabilité JSON
        out.setdefault("answer", "Je ne sais pas d’après les données disponibles.")
        out.setdefault("sources", [])
        out.setdefault("suggested_actions", [])
        out.setdefault("navigation", [])
        out.setdefault("used", {})
        return out
    except Exception as e:
        return {
            "answer": "Erreur interne côté serveur.",
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": {"mode": "error", "error": repr(e), "debug": req.debug},
        }
