from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

RX_SUM   = re.compile(r"\b(total|somme|global|montant\s+total)\b", re.I)
RX_COUNT = re.compile(r"\b(nombre|combien|count)\b", re.I)
RX_AVG   = re.compile(r"\b(moyenne|average)\b", re.I)
RX_MIN   = re.compile(r"\b(minimum|min)\b", re.I)
RX_MAX   = re.compile(r"\b(maximum|max)\b", re.I)

RX_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

RX_NOMME = re.compile(r"\b(nomm[eé]|appel[eé])\s+(.+?)(\?|$)", re.I)
RX_QUOTE = re.compile(r'"([^"]{3,})"')

RX_REL = re.compile(
    r"\b(qui\s+est|quel(le)?\s+est)\s+(?P<attr>[a-zA-Z_éèêàç\- ]{2,}?)\s+(du|de\s+la|de\s+l'|de)\s+(?P<entity>[a-zA-Z_]+)",
    re.I
)

RX_FUND_NAME = re.compile(
    r"\b(?:fonds|fond)\b(?:\s+(?:nomm[eé]|appel[eé]))?\s+(?P<name>.+?)(?:\s+(?:pour|en|de|du|des)\b|[?]|$)",
    re.I
)

STOP_ARTICLES = re.compile(r"^(le|la|les|l')\s+", re.I)

@dataclass
class AnalyticsIntent:
    kind: str
    agg: Optional[str] = None
    year: Optional[int] = None
    entity_table: Optional[str] = None
    entity_name: Optional[str] = None
    attr_table: Optional[str] = None
    fund_name: Optional[str] = None
    raw: str = ""

def extract_year(q: str) -> Optional[int]:
    m = RX_YEAR.search(q or "")
    return int(m.group(1)) if m else None

def extract_entity_name(q: str) -> Optional[str]:
    q0 = (q or "").strip()
    m = RX_QUOTE.search(q0)
    if m:
        return m.group(1).strip()
    m = RX_NOMME.search(q0)
    if m:
        return m.group(2).strip().strip('"').strip("'")
    return None

def extract_name_after_entity(q: str, entity: str) -> Optional[str]:
    q0 = (q or "").strip()
    if not entity:
        return None
    m = re.search(rf"\b{re.escape(entity)}\b\s+(?P<name>.+?)(?:\?|$)", q0, re.I)
    if not m:
        return None
    name = (m.group("name") or "").strip()
    name = re.sub(r"\s+", " ", name).strip().strip('"\'')

    name = re.sub(r"^(nomm[eé]|appel[eé])\s+", "", name, flags=re.I).strip()

    name = re.sub(r"\b(pour|en|de|du|des)\b.*$", "", name, flags=re.I).strip()
    return name if len(name) >= 3 else None

def extract_fund_name(q: str) -> Optional[str]:
    q0 = (q or "").strip()
    if re.search(r"\b(fonds|fond)\b", q0, re.I):
        m = RX_QUOTE.search(q0)
        if m:
            return m.group(1).strip()

    m = RX_FUND_NAME.search(q0)
    if not m:
        return None

    name = (m.group("name") or "").strip()
    name = re.sub(r"\s+", " ", name).strip().strip('"\'')

    name = re.sub(r"\b(pour|en|de|du|des)\b.*$", "", name, flags=re.I).strip()
    return name if len(name) >= 3 else None

def normalize_attr(attr: str) -> str:
    a = (attr or "").strip()
    a = STOP_ARTICLES.sub("", a).strip()
    a = re.sub(r"\s+", " ", a)
    return a.replace(" ", "_").replace("-", "_").lower()

def detect_agg(q: str) -> Optional[str]:
    q0 = q or ""
    if RX_SUM.search(q0):
        return "sum"
    if RX_COUNT.search(q0):
        return "count"
    if RX_AVG.search(q0):
        return "avg"
    if RX_MIN.search(q0):
        return "min"
    if RX_MAX.search(q0):
        return "max"
    return None

def parse_intent(message: str) -> Optional[AnalyticsIntent]:
    q = message or ""

    m = RX_REL.search(q)
    if m:
        attr_raw = (m.group("attr") or "").strip()
        entity = (m.group("entity") or "").strip().lower()

        name = extract_entity_name(q)
        if not name:
            name = extract_name_after_entity(q, entity)

        return AnalyticsIntent(
            kind="rel",
            entity_table=entity,
            entity_name=name,
            attr_table=normalize_attr(attr_raw),
            year=extract_year(q),
            fund_name=extract_fund_name(q),
            raw=q,
        )

    agg = detect_agg(q)
    if agg:
        return AnalyticsIntent(
            kind="agg",
            agg=agg,
            year=extract_year(q),
            fund_name=extract_fund_name(q),
            raw=q,
        )

    return None
