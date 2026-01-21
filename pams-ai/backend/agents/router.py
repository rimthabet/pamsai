# agents/router.py
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class AgentPlan:
    intent: str              # qa | guide | crud | report
    domain: str              # fonds | projet | souscription | liberation | document | general
    source_types: List[str]  
    entity_hint: Optional[str] = None


DOMAIN_TO_SOURCE_TYPES = {
    "fonds": ["maxula:fonds"],
    "projet": ["maxula:projet", "maxula:projects", "maxula:projets"],
    "souscription": ["maxula:souscription", "maxula:souscriptions"],
    "liberation": ["maxula:liberation", "maxula:liberations"],
    "document": ["pdf_ocr", "maxula:document"],
    "general": [],
}

# Intent
RX_REPORT = re.compile(r"\b(rapport|reporting|export|excel|pdf|hebdo|mensuel|trimestriel)\b", re.I)
RX_CRUD   = re.compile(r"\b(ajoute|ajouter|crée|creer|modifier|mets à jour|mettre à jour|supprimer)\b", re.I)
RX_GUIDE  = re.compile(r"\b(comment|où|ou|etapes|étapes|procédure|procedure|guide)\b", re.I)

# Domain
RX_DOC   = re.compile(r"\b(prospectus|pv|proc[eè]s[-\s]?verbal|r[eè]glement|note|annexe|risque|risques|document)\b", re.I)
RX_FONDS = re.compile(r"\b(fonds|fcpr|fcp|visa|cmf|agrement|agrément|ratio|frais|montant|durée|duree)\b", re.I)
RX_PROJ  = re.compile(r"\b(projet|pipeline|prospection|préselection|présélection|etude|étude|business\s*plan|bp|activité|activite|capital)\b", re.I)
RX_SOUS  = re.compile(r"\b(souscription|souscriptions|souscrire|souscripteur|investisseur|engagement)\b", re.I)
RX_LIB   = re.compile(r"\b(libération|liberation|appel\s+de\s+fonds|tirage|versement)\b", re.I)

# Simple extraction d’entité (nom après “nommé …” / “appelé …” / guillemets)
RX_ENTITY = re.compile(r"(?:nommé|nommee|appelé|appele)\s+(.+)$|\"([^\"]+)\"", re.I)


def detect_intent(q: str) -> str:
    q = q or ""
    if RX_REPORT.search(q):
        return "report"
    if RX_CRUD.search(q):
        return "crud"
    if RX_GUIDE.search(q):
        return "guide"
    return "qa"


def detect_domain(q: str) -> str:
    q = q or ""
    # priorité doc si keywords doc
    if RX_DOC.search(q):
        return "document"
    if RX_LIB.search(q):
        return "liberation"
    if RX_SOUS.search(q):
        return "souscription"
    if RX_PROJ.search(q):
        return "projet"
    if RX_FONDS.search(q):
        return "fonds"
    return "general"


def extract_entity_hint(q: str) -> Optional[str]:
    q = (q or "").strip()
    m = RX_ENTITY.search(q)
    if not m:
        return None
    g1 = (m.group(1) or "").strip() if m.group(1) else ""
    g2 = (m.group(2) or "").strip() if m.group(2) else ""
    val = g1 or g2
    # nettoyer ponctuation finale
    val = re.sub(r"[?.!,;:]+$", "", val).strip()
    return val or None


def build_plan(q: str) -> AgentPlan:
    intent = detect_intent(q)
    domain = detect_domain(q)

    # scopes
    source_types = DOMAIN_TO_SOURCE_TYPES.get(domain, [])

    # cas “frais + commissions” => doc + fonds
    if re.search(r"\bfrais\b", q or "", re.I) and re.search(r"\bcommission", q or "", re.I):
        source_types = list(dict.fromkeys(DOMAIN_TO_SOURCE_TYPES["document"] + DOMAIN_TO_SOURCE_TYPES["fonds"]))
        domain = "document"

    return AgentPlan(
        intent=intent,
        domain=domain,
        source_types=source_types,
        entity_hint=extract_entity_hint(q),
    )
