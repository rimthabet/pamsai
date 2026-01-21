from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    session_id: Optional[str] = None
    role: str = "analyst"  
    top_k: int = 8
    model: str = "llama3.2"
    debug: bool = False


class SourceItem(BaseModel):
    id: int
    source_type: str
    source_id: str
    score: float
    metadata: Dict[str, Any] = {}


class ActionItem(BaseModel):
    type: str
    payload: Dict[str, Any] = {}


class ChatResponse(BaseModel):
    answer: str
    sources: List[SourceItem] = []
    suggested_actions: List[ActionItem] = []
    navigation: List[ActionItem] = []
    used: Dict[str, Any] = {}   


class ConfirmActionRequest(BaseModel):
    session_id: Optional[str] = None
    role: str = "analyst"
    action: ActionItem


class ConfirmActionResponse(BaseModel):
    status: str
    message: str
    result: Dict[str, Any] = {}
