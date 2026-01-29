from __future__ import annotations

import json
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.chat_service import chat_pipeline
from rag.retrieve_core import _get_embed_model


class UTF8JSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, ensure_ascii=False).encode("utf-8")


app = FastAPI(title="PAMS-AI Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def warmup_models():
    _get_embed_model()


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


@app.post("/chat")
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

        
        out.setdefault("answer", "Je ne sais pas d’après les données disponibles.")
        out.setdefault("sources", [])
        out.setdefault("suggested_actions", [])
        out.setdefault("navigation", [])
        out.setdefault("used", {})

       
        return UTF8JSONResponse(content=out)

    except Exception as e:
        err = {
            "answer": "Erreur interne côté serveur.",
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": {"mode": "error", "error": repr(e), "debug": req.debug},
        }
        return UTF8JSONResponse(content=err, status_code=500)
