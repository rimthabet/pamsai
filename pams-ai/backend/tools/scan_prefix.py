import os
import requests
from itertools import product

BASE = os.getenv("PAMS_API_BASE_URL", "https://127.0.0.1:7443").rstrip("/")
VERIFY_SSL = os.getenv("PAMS_API_VERIFY_SSL", "0") == "1"
TIMEOUT = float(os.getenv("PAMS_API_TIMEOUT", "2.5")) 
HOST_HEADER = os.getenv("PAMS_API_HOST", "").strip()   

# prefixes probables
PREFIXES = [
    "", "api", "rest", "backend", "service", "services",
    "pams", "pams-api", "gateway",
    "v1", "v2", "v3",
    "pams/api", "backend/api", "api/v1", "api/v2", "api/v3",
    "pams/api/v1", "pams/api/v2", "pams/api/v3",
]

# endpoints probes (courants)
PROBES = [
    "", "/", "health", "actuator/health",
    "swagger-ui", "swagger-ui/index.html",
    "v3/api-docs", "openapi.json",
    "docs", "redoc",
    "auth", "auth/login", "login",
    "me", "users/me",
    "fonds", "funds", "projets", "projects",
]

def norm(p: str) -> str:
    p = (p or "").strip()
    p = p.strip("/")
    return p

def build_url(prefix: str, probe: str) -> str:
    prefix = norm(prefix)
    probe = (probe or "").strip().lstrip("/")
    if prefix and probe:
        return f"{BASE}/{prefix}/{probe}"
    if prefix and not probe:
        return f"{BASE}/{prefix}"
    if (not prefix) and probe:
        return f"{BASE}/{probe}"
    return f"{BASE}/"

def hit(status: int) -> bool:
    return (200 <= status < 400) or status in (401, 403)

def req(url: str):
    headers = {"User-Agent": "pams-ai-agent/scan"}
    if HOST_HEADER:
        headers["Host"] = HOST_HEADER

    try:
        # HEAD d'abord = plus rapide (fallback GET si pas supporté)
        r = requests.head(url, timeout=TIMEOUT, verify=VERIFY_SSL, allow_redirects=False, headers=headers)
        if r.status_code == 405: 
            r = requests.get(url, timeout=TIMEOUT, verify=VERIFY_SSL, allow_redirects=False, headers=headers)
        ct = (r.headers.get("content-type", "") or "").split(";")[0].strip()
        return r.status_code, ct
    except Exception:
        return None, None

def main():
    print("BASE =", BASE)
    print("verify_ssl =", VERIFY_SSL)
    print("timeout =", TIMEOUT)
    if HOST_HEADER:
        print("host_header =", HOST_HEADER)
    print("---- scanning FAST ----")

    
    candidates = set(PREFIXES)
    bases2 = ["pams", "backend", "gateway"]
    for b in bases2:
        for v in ["", "v1", "v2", "v3"]:
            for a in ["api", "rest", ""]:
                parts = [x for x in [b, a, v] if x]
                candidates.add("/".join(parts))

    found = []
    tested = 0

    for pref in sorted(candidates):
        for pr in PROBES:
            url = build_url(pref, pr)
            tested += 1
            code, ct = req(url)
            if code is None:
                continue

            if hit(code):
                path = "/" + "/".join([x for x in [norm(pref), pr.strip("/")] if x])
                found.append((code, ct, path, url))
                print(f"[HIT {code}] {ct:18} {path:35} -> {url}")

              
                if len(found) >= 12:
                    print("\nAssez de hits pour déduire le prefix.")
                    return

    print(f"\nTerminé. tested={tested} hits={len(found)}")
    if not found:
        print(" Aucun hit détecté.")
        print("Très probable: l'API n'est PAS accessible en GET/HEAD sur ce port OU nécessite un Host spécifique.")
        print("Solution sûre: récupérer une URL XHR depuis Angular (Network -> Fetch/XHR -> Request URL).")

if __name__ == "__main__":
    main()
