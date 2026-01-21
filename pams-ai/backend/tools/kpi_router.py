# tools/kpi_router.py
import re
from typing import Optional

RX_TOTAL = re.compile(r"\b(total|somme|global)\b", re.I)
RX_MONTANT = re.compile(r"\b(montant|valeur|encours)\b", re.I)

RX_ACTIF = re.compile(r"\b(actif|actifs)\b", re.I)
RX_INVESTI = re.compile(r"\b(investi|investis|investissement|investissements)\b", re.I)

RX_FONDS = re.compile(r"\b(fonds|fcpr|fcp)\b", re.I)

RX_ACTIONS = re.compile(r"\b(action|actions)\b", re.I)
RX_OCA = re.compile(r"\b(oca)\b", re.I)
RX_CCA = re.compile(r"\b(cca)\b", re.I)


def route_kpi(message: str) -> Optional[str]:
    q = (message or "").strip()

    if RX_FONDS.search(q) and (RX_TOTAL.search(q) or RX_MONTANT.search(q)):
        return "total_montant_fonds"

    if RX_TOTAL.search(q) and RX_ACTIF.search(q):
        return "total_actif"
    if RX_INVESTI.search(q) and RX_ACTIONS.search(q):
        return "investi_actions"
    if RX_INVESTI.search(q) and RX_OCA.search(q):
        return "investi_oca"
    if RX_INVESTI.search(q) and RX_CCA.search(q):
        return "investi_cca"

    if RX_TOTAL.search(q) and RX_INVESTI.search(q):
        return "total_investi"

    if RX_INVESTI.search(q) and RX_TOTAL.search(q):
        return "total_investi"

    return None
