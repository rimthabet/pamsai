from __future__ import annotations

import os
import json
import argparse
from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Tuple

import requests


# -----------------------------
# Config
# -----------------------------
DEFAULT_BASE = "https://127.0.0.1:7443"

PAMS_API_BASE_URL = os.getenv("PAMS_API_BASE_URL", DEFAULT_BASE).rstrip("/")
PAMS_API_TOKEN = os.getenv("PAMS_API_TOKEN", "").strip()  
PAMS_API_COOKIE = os.getenv("PAMS_API_COOKIE", "").strip()  
PAMS_API_VERIFY_SSL = os.getenv("PAMS_API_VERIFY_SSL", "0").strip() == "1"  
PAMS_API_TIMEOUT = float(os.getenv("PAMS_API_TIMEOUT", "10"))

DEFAULT_ROLE = os.getenv("PAMS_ROLE", "analyst")  

# -----------------------------
# Errors
# -----------------------------
class PamsApiError(RuntimeError):
    pass


# -----------------------------
# Helpers
# -----------------------------
def _join_url(base: str, path: str) -> str:
    base = (base or "").rstrip("/")
    path = (path or "").lstrip("/")
    if not path:
        return base + "/"
    return f"{base}/{path}"


def _headers(role: str = DEFAULT_ROLE) -> Dict[str, str]:
    h = {
        "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
        "User-Agent": "pams-ai-agent/1.0",
        "X-User-Role": role,  
    }

    if PAMS_API_TOKEN:
        h["Authorization"] = f"Bearer {PAMS_API_TOKEN}"

    if PAMS_API_COOKIE:
        h["Cookie"] = PAMS_API_COOKIE

    return h


def _maybe_json(r: requests.Response) -> Any:
    ct = (r.headers.get("content-type") or "").lower()
    if "application/json" in ct:
        try:
            return r.json()
        except Exception:
            return r.text
    return r.text


def _request(method: str, path: str, role: str = DEFAULT_ROLE, json_body: Any = None) -> Any:
    url = _join_url(PAMS_API_BASE_URL, path)

    try:
        r = requests.request(
            method=method.upper(),
            url=url,
            headers=_headers(role),
            json=json_body,
            timeout=PAMS_API_TIMEOUT,
            verify=PAMS_API_VERIFY_SSL,
            allow_redirects=False,  
        )
    except requests.RequestException as e:
        raise PamsApiError(f"{method.upper()} {url} failed: {e}")

    if r.status_code >= 400:
        
        preview = (r.text or "")[:800]
        raise PamsApiError(f"{method.upper()} {url} -> {r.status_code}: {preview}")

    return _maybe_json(r)


# -----------------------------
# Public API
# -----------------------------
def pams_api_get(path: str, role: str = DEFAULT_ROLE) -> Any:
    return _request("GET", path, role=role)


def pams_api_post(path: str, payload: Any, role: str = DEFAULT_ROLE) -> Any:
    return _request("POST", path, role=role, json_body=payload)


def pams_api_put(path: str, payload: Any, role: str = DEFAULT_ROLE) -> Any:
    return _request("PUT", path, role=role, json_body=payload)


def pams_api_delete(path: str, role: str = DEFAULT_ROLE) -> Any:
    return _request("DELETE", path, role=role)


# -----------------------------
# Discover
# -----------------------------
@dataclass(frozen=True)
class ProbeResult:
    status: int
    path: str
    content_type: str
    url: str


def discover_endpoints(prefix: str = "") -> List[ProbeResult]:
    """
    Essaie des endpoints standards.
    IMPORTANT: compte aussi 401/403/302 comme "hit" car l'API peut être protégée.
    """
    base = PAMS_API_BASE_URL.rstrip("/")
    prefix = (prefix or "").strip()
    if prefix and not prefix.startswith("/"):
        prefix = "/" + prefix
    prefix = prefix.rstrip("/")

    candidates = [
        "", "/",  
        "/api", "/api/",
        "/health", "/actuator/health",
        "/swagger-ui", "/swagger-ui/", "/swagger-ui/index.html",
        "/v3/api-docs", "/openapi.json",
        "/docs", "/redoc",
    ]

    hits: List[ProbeResult] = []
    for p in candidates:
        path = (prefix + p) if p else (prefix or "/")
        # normaliser
        url = _join_url(base + prefix, p.lstrip("/")) if p else _join_url(base, prefix.lstrip("/"))

        try:
            r = requests.get(
                url,
                headers=_headers(DEFAULT_ROLE),
                timeout=PAMS_API_TIMEOUT,
                verify=PAMS_API_VERIFY_SSL,
                allow_redirects=False,
            )
            ct = (r.headers.get("content-type") or "").split(";")[0].strip()

            
            if (200 <= r.status_code < 400) or r.status_code in (401, 403):
                hits.append(ProbeResult(r.status_code, path, ct, url))
        except Exception:
            continue

    # dédoublonnage (url unique)
    uniq = {}
    for h in hits:
        uniq[h.url] = h
    return list(sorted(uniq.values(), key=lambda x: (x.status, x.url)))


# -----------------------------
# CLI
# -----------------------------
def _pretty_print(obj: Any):
    if isinstance(obj, (dict, list)):
        print(json.dumps(obj, ensure_ascii=False, indent=2))
    else:
        print(obj)


def _cli():
    parser = argparse.ArgumentParser(description="PAMS API client (tools)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_disc = sub.add_parser("discover", help="Probe standard endpoints")
    p_disc.add_argument("--prefix", default="", help="Prefix optionnel (ex: pams, backend, api)")

    p_get = sub.add_parser("get", help="GET path")
    p_get.add_argument("path", help="ex: pams/api/fonds")
    p_get.add_argument("--role", default=DEFAULT_ROLE)

    p_post = sub.add_parser("post", help="POST path with JSON payload")
    p_post.add_argument("path")
    p_post.add_argument("--role", default=DEFAULT_ROLE)
    p_post.add_argument("--json", required=True, help='JSON string, ex: \'{"a":1}\'')

    p_put = sub.add_parser("put", help="PUT path with JSON payload")
    p_put.add_argument("path")
    p_put.add_argument("--role", default=DEFAULT_ROLE)
    p_put.add_argument("--json", required=True)

    p_del = sub.add_parser("delete", help="DELETE path")
    p_del.add_argument("path")
    p_del.add_argument("--role", default=DEFAULT_ROLE)

    args = parser.parse_args()

    if args.cmd == "discover":
        hits = discover_endpoints(prefix=args.prefix)
        if not hits:
            print("Aucun endpoint standard trouvé. Essaie un prefix (ex: pams, backend) ou copie une URL XHR depuis Angular.")
            return
        for h in hits:
            print(f"[{h.status}] {h.path:30} -> {h.url} ({h.content_type or 'n/a'})")
        return

    if args.cmd == "get":
        res = pams_api_get(args.path, role=args.role)
        _pretty_print(res)
        return

    if args.cmd == "post":
        payload = json.loads(args.json)
        res = pams_api_post(args.path, payload, role=args.role)
        _pretty_print(res)
        return

    if args.cmd == "put":
        payload = json.loads(args.json)
        res = pams_api_put(args.path, payload, role=args.role)
        _pretty_print(res)
        return

    if args.cmd == "delete":
        res = pams_api_delete(args.path, role=args.role)
        _pretty_print(res)
        return


if __name__ == "__main__":
    _cli()
