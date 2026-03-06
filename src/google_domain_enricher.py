#!/usr/bin/env python3
"""
CraneGenius Domain Enricher v3
Targets enriched_companies.csv rows where domain_resolution_source = 'unresolved' or 'enrichment_generated'
Primary:  Google Custom Search API (100 free/day)
Fallback: DuckDuckGo HTML scrape (unlimited, no key needed)
"""
import csv, logging, os, re, time
from pathlib import Path
from urllib.parse import urlparse, quote_plus
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)
log = logging.getLogger("cranegenius.domain_enricher")
logging.basicConfig(level=logging.INFO, format="%(message)s")

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
GOOGLE_CX      = os.environ.get("GOOGLE_CSE_ID", "")
SERPAPI_KEY    = os.environ.get("SERPAPI_KEY", "")
ENRICHED_CSV   = Path(__file__).parent.parent / "data/enriched_companies.csv"
OUTPUT_CSV     = Path(__file__).parent.parent / "data/google_enriched_domains.csv"

SKIP_DOMAINS = {
    "buildzoom.com","claimspages.com","inforuptcy.com","dallasopendata.com","visitdallas.com","usps.com","dallasnews.com","pcn.procore.com","pcn.procoretech-qa.com","govcb.com","govconinabox.com","har.com","ship-express.com","theagencyre.com","dfwmoves.com","document.epiq11.com","content.civicplus.com","tripmasters.com","familytreenow.com","nationalpublicdata.com","eb.dallasbuilders.com","rreaf.com","claconnect.com","titanretail.com","carbonenviro.com","curbio-dallas.com","dallasnews.com","beckgroup.com","apartments.com","homes.com","zillow.com","realtor.com","redfin.com","trulia.com","thehabeshaweb.com","empresasdirectorio.com","negocios.com","niche.com","thebluebook.com","dallascityhall.com",
    "berkeleyelectric.coop","constructconnect.com","ispot.tv",
    "chamberofcommerce.com","chamberofcommerce.org","cityof",
    "expertise.com","pagesjaunes.fr","showmelocal.com","superpages.com",
    "mapquest.com","whitepages.com","spokeo.com","credibly.com",
    "opengovus.com","bizstats.com","remodeling.hw.net","remodelingmagazine.com",
    "duckduckgo.com",
    "facebook.com","linkedin.com","yelp.com","yellowpages.com","bbb.org",
    "angieslist.com","houzz.com","thumbtack.com","homeadvisor.com","porch.com",
    "birdeye.com","google.com","bing.com","yahoo.com","wikipedia.org",
    "manta.com","dnb.com","zoominfo.com","mapquest.com","instagram.com","twitter.com","procore.com","boostedmedia.net","zabalist.com","bizapedia.com","bldup.com","loopnet.com","importgenius.com","levelset.com","privco.com","choosechicago.com","compass.com","youtube.com","instacart.com","chicagoyimby.com","chicago.urbanize.city","chicagocityscape.com","atproperties.com","alshgroup.com","luxeredawards.com","solutions.lg.com","globalsportscamp.com"
}
DDG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

SKIP_KEYWORDS = {"directory","listing","yellow","whitepage","chamber","cityof","opendata","bluebook","buildzoom","niche","expertise","superpages","showmelocal"}

def _is_biz(url):
    try:
        d = urlparse(url if url.startswith("http") else "https://"+url).netloc.lower().lstrip("www.")
        return not any(s in d for s in SKIP_DOMAINS) and not any(d.endswith(t) for t in (".gov",".edu",".org",".mil",".law"))
    except: return False

def _domain(url):
    try: return urlparse(url if url.startswith("http") else "https://"+url).netloc.lower().lstrip("www.").split("/")[0]
    except: return ""

def _clean_name(raw):
    # First: if there is a number followed by a street, cut everything from there
    raw = re.sub(r"\s+\d+\s+[nesw]?\.?\s*\w+\s+(rd|st|ave|blvd|dr|ln|way|ct|pl|hwy|pkwy|ste|suite|#|north|south|east|west|nw|ne|sw|se).*$", "", raw, flags=re.I)
    raw = re.sub(r"\s+p\.?\s*o\.?\s*box.*$", "", raw, flags=re.I)
    raw = re.sub(r",.*$", "", raw)
    raw = re.sub(r"\(?\d{3}\)?\s*[\-\.]?\s*\d{3}[\-\.\s]\d{4}", "", raw)
    raw = re.sub(r"(llc|inc|corp|co|ltd)\.?", "", raw, flags=re.I)
    return re.sub(r"\s+", " ", raw).strip()
def _clean_name_ORIG(raw):
    """Strip address/phone noise from contractor_name_normalized."""
    # Remove phone numbers
    c = re.sub(r'\(?\d{3}\)?[\s\-]\d{3}[\s\-]\d{4}', '', raw)
    # Remove address patterns (number + street)
    c = re.sub(r'\s+\d+\s+\w+\s+(rd|st|ave|blvd|dr|ln|way|ct|pl|hwy|pkwy).*$', '', c, flags=re.I)
    # Remove P.O. Box
    c = re.sub(r'p\.?o\.?\s+box.*$', '', c, flags=re.I)
    # Remove city/state/zip at end
    c = re.sub(r',\s*[a-z\s]+,\s*(tx|il|ny|ca|fl|az|wa|or)\s*\d*$', '', c, flags=re.I)
    # Remove commas and extra whitespace
    c = re.sub(r',.*$', '', c)
    c = re.sub(r'\b(LLC|Inc|Corp|Co|Ltd)\b\.?', '', c, flags=re.I)
    return c.strip()

def _query(raw_name, city=""):
    name = _clean_name(raw_name)
    suffix = f" {city}" if city else " contractor"
    return f"{name}{suffix}"

def google_search(q):
    if not GOOGLE_API_KEY or not GOOGLE_CX: return None
    try:
        data = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": q, "num": 5},
            timeout=10
        ).json()
        if "error" in data:
            msg = data["error"].get("message","")
            if "does not have" in msg or "quota" in msg.lower(): return "UNAVAILABLE"
            return None
        for item in data.get("items",[]):
            if _is_biz(item.get("link","")): return _domain(item["link"])
    except: pass
    return None

def serp_search(q):
    if not SERPAPI_KEY: return None
    try:
        from serpapi import GoogleSearch
        results = GoogleSearch({"q": q, "api_key": SERPAPI_KEY, "num": 5}).get_dict()
        for r in results.get("organic_results", []):
            url = r.get("link","")
            if _is_biz(url): return _domain(url)
    except Exception as e:
        log.debug(f"  [SerpAPI] {e}")
    return None

def run(limit, serp_only=False, google_only=False):
    with open(ENRICHED_CSV) as f:
        rows = list(csv.DictReader(f))

    target_sources = {"unresolved", "enrichment_generated", ""}
    # Deduplicate by normalized name — same company appears on multiple permits
    seen = set()
    targets = []
    for r in rows:
        if r.get("domain_resolution_source","").strip() not in target_sources:
            continue
        key = r.get("contractor_name_normalized","").strip().lower()
        if key and key not in seen:
            seen.add(key)
            targets.append(r)
    batch = targets[:limit]

    log.info(f"\n  [Enricher v3] {len(targets)} targets (unresolved + generated) → running {len(batch)}")
    log.info(f"  Mode: {'SerpAPI only' if serp_only else 'Google only' if google_only else 'Google → DDG fallback'}\n")

    resolved_map = {}
    g_hits = ddg_hits = s_hits = misses = 0
    g_dead = False

    for i, row in enumerate(batch, 1):
        raw_name = row.get("contractor_name_normalized", "").strip()
        city     = row.get("project_city", "").strip()
        if not raw_name:
            misses += 1
            continue

        q = _query(raw_name, city)
        domain = source = None

        if not serp_only and not g_dead:
            res = google_search(q)
            if res == "UNAVAILABLE":
                g_dead = True
                log.info("  [!] Google unavailable → DDG fallback for all remaining")
            elif res:
                domain = res; source = "google"; g_hits += 1

        if domain is None and not google_only:
            domain = serp_search(q)
            if domain: source = "ddg"; ddg_hits += 1
            else: misses += 1

        if domain:
            resolved_map[row.get("dedupe_key","") or raw_name] = (domain, source)

        status = f"✓ {source:<6} → {domain}" if domain else "✗ miss"
        log.info(f"  [{i}/{len(batch)}] {status}  |  {raw_name[:50]}")

        if not google_only:
            time.sleep(1.2)

    # Write output CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not OUTPUT_CSV.exists()
    with open(OUTPUT_CSV, "a", newline="") as f:
        import csv as csv2
        w = csv2.DictWriter(f, fieldnames=["contractor_name_normalized","contractor_domain","source","query","dedupe_key"])
        if write_header: w.writeheader()
        for row in batch:
            key = row.get("dedupe_key","") or row.get("contractor_name_normalized","")
            if key in resolved_map:
                domain, src = resolved_map[key]
                w.writerow({
                    "contractor_name_normalized": row.get("contractor_name_normalized",""),
                    "contractor_domain": domain,
                    "source": src,
                    "query": _query(row.get("contractor_name_normalized",""), row.get("project_city","")),
                    "dedupe_key": key
                })

    # Patch enriched_companies.csv with resolved domains
    updated = 0
    for row in rows:
        key = row.get("dedupe_key","") or row.get("contractor_name_normalized","")
        if key in resolved_map and row.get("domain_resolution_source","").strip() in target_sources:
            domain, src = resolved_map[key]
            row["contractor_domain"] = domain
            row["domain_resolution_source"] = f"ddg_resolved" if src == "ddg" else "google_resolved"
            updated += 1

    with open(ENRICHED_CSV, "w", newline="") as f:
        import csv as csv2
        w = csv2.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    total = g_hits + ddg_hits + s_hits
    log.info(f"\n  ── Results ───────────────────────────────────────")
    log.info(f"  Google : {g_hits}  |  DDG : {ddg_hits}  |  SerpAPI : {s_hits}  |  Misses : {misses}")
    log.info(f"  Total  : {total}/{len(batch)} ({100*total//len(batch) if batch else 0}% hit rate)")
    log.info(f"  Patched enriched_companies.csv: {updated} rows updated")
    log.info(f"  Output : {OUTPUT_CSV}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("limit", type=int, nargs="?", default=20)
    p.add_argument("--serp-only", action="store_true")
    p.add_argument("--google-only", action="store_true")
    a = p.parse_args()
    run(a.limit, serp_only=a.serp_only, google_only=a.google_only)
