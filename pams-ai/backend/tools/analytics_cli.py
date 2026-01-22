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
    r"\b(qui\s+est|quel(le)?\s+est)\s+(?P<attr>[a-zA-Z_éèêàç\- ]{2,}?)\s+(du|de\s+la|de\s+l')\s+(?P<entity>[a-zA-Z_]+)",
    re.I
)

RX_FUND = re.compile(r"\b(fond|fonds)\s+(?P<name>.+)$", re.I)


@dataclass
class AnalyticsIntent:
    kind: str
    agg: Optional[str] = None
    year: Optional[int] = None
    fund_name: Optional[str] = None
    entity_table: Optional[str] = None
    entity_name: Optional[str] = None
    attr_table: Optional[str] = None
    raw: str = ""


def extract_year(q: str) -> Optional[int]:
    m = RX_YEAR.search(q or "")
    return int(m.group(1)) if m else None


def extract_entity_name(q: str) -> Optional[str]:
    q = (q or "").strip()
    m = RX_QUOTE.search(q)
    if m:
        return m.group(1).strip()
    m = RX_NOMME.search(q)
    if m:
        return m.group(2).strip().strip('"').strip("'")
    return None


def extract_fund_name(q: str) -> Optional[str]:
    q = (q or "").strip()
    m = RX_QUOTE.search(q)
    if m:
        return m.group(1).strip()
    m = RX_FUND.search(q)
    if not m:
        return None
    name = (m.group("name") or "").strip()
    name = re.split(r"\b(pour|en|dans|sur)\b", name, maxsplit=1, flags=re.I)[0].strip()
    name = re.split(r"\b(19\d{2}|20\d{2})\b", name, maxsplit=1)[0].strip()
    return name.strip('"').strip("'")


def detect_agg(q: str) -> Optional[str]:
    q = q or ""
    if RX_SUM.search(q):
        return "sum"
    if RX_COUNT.search(q):
        return "count"
    if RX_AVG.search(q):
        return "avg"
    if RX_MIN.search(q):
        return "min"
    if RX_MAX.search(q):
        return "max"
    return None


def parse_intent(message: str) -> Optional[AnalyticsIntent]:
    q = message or ""

    m = RX_REL.search(q)
    if m:
        attr = (m.group("attr") or "").strip().lower()
        entity = (m.group("entity") or "").strip().lower()
        name = extract_entity_name(q)
        return AnalyticsIntent(
            kind="rel",
            entity_table=entity,
            entity_name=name,
            attr_table=attr.replace(" ", "_").replace("-", "_"),
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
