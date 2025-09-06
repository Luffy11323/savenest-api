# filename: live_price_api.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Live Price Comparison API (no DB) — Pakistan-focused

- FastAPI endpoint /search?q=... returns live prices (min→max)
- Hits multiple stores in parallel; JSON-LD first, CSS fallback
- Query expansion via Shopify suggestions + optional brand hints
- Optional JS rendering (Playwright) for heavy JS sites (off by default)
- Small in-memory TTL cache to avoid hammering sites during bursts

Run:
  uvicorn live_price_api:app --host 0.0.0.0 --port 8000
"""

import asyncio, time, re, json, os
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, urlparse, quote_plus
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from rapidfuzz import fuzz

# --------------------------
# Config
# --------------------------
USE_PLAYWRIGHT = os.getenv("USE_PLAYWRIGHT", "0") == "1"  # set USE_PLAYWRIGHT=1 to enable
PER_HOST_GAP = float(os.getenv("PER_HOST_GAP", "0.9"))    # politeness per host (seconds)
MAX_LINKS_PER_STORE = int(os.getenv("MAX_LINKS_PER_STORE", "8"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "28"))
TTL_SECONDS = int(os.getenv("TTL_SECONDS", "180"))        # in-memory cache TTL per URL (seconds)

# Stores to hit live (extend freely)
STORES = [
    # Groceries / household
    {"base": "https://www.naheed.pk", "platform": "shopify"},
    {"base": "https://alfatah.pk", "platform": "shopify"},
    {"base": "https://esajee.com", "platform": "shopify"},
    {"base": "https://www.metro-online.pk", "platform": "woo"},
    {"base": "https://www.carrefour.pk/mafpak/en", "platform": "custom",
     "search": "https://www.carrefour.pk/mafpak/en/search?q={q}",
     "product_link_patterns": [r"/mafpk/en/p/[-a-z0-9]+"]},

    # General / electronics
    {"base": "https://telemart.pk", "platform": "woo"},
    {"base": "https://homeshopping.pk", "platform": "woo"},
    {"base": "https://www.shophive.com", "platform": "woo"},
    {"base": "https://www.ishopping.pk", "platform": "woo"},
    {"base": "https://www.galaxy.pk", "platform": "woo"},
    {"base": "https://www.mega.pk", "platform": "custom",
     "search": "https://www.mega.pk/search/?q={q}",
     "product_link_patterns": [r"/items/[-a-z0-9]+\\.html$"],
     "product_fallback": {"title": "h1", "price": ".price, .Price"}},

    # Marketplace / aggregator
    {"base": "https://www.daraz.pk", "platform": "custom",
     "search": "https://www.daraz.pk/catalog/?q={q}",
     "product_link_patterns": [r"/products?/[-a-z0-9]+\\.html", r"/products?/.+?-i\\d+\\.html"],
     "product_fallback": {"title": "h1.pdp-mod-product-title, h1.pdp-mod-product-badge-title",
                          "price": "span.pdp-price, span.pdp-mod-product-price"}},
    {"base": "https://priceoye.pk", "platform": "custom",
     "search": "https://priceoye.pk/search?q={q}",
     "product_link_patterns": [r"/product/[-a-z0-9]+", r"/[a-z0-9-]+/price$"],
     "product_fallback": {"title": "h1, h1.product-name", "price": "div.price-box span, .product-price, span.price"}},

    # Sports (sample)
    {"base": "https://sportsplus.pk", "platform": "woo"},
    {"base": "https://sportskit.pk", "platform": "woo"},
    {"base": "https://www.sanasports.com.pk", "platform": "woo"},
    {"base": "https://www.decathlon.pk", "platform": "custom",
     "search": "https://www.decathlon.pk/search?q={q}",
     "product_link_patterns": [r"/products?/[-a-z0-9]+"]},
]

# Optional domain-specific boosters for vague queries
BRAND_HINTS: Dict[str, List[str]] = {
    "bread": ["dawn bread", "bake parlour bread", "whole wheat bread", "bran bread"],
    "milk": ["olpers milk", "milkpak", "nestle milk", "dayfresh milk"],
    "sugar": ["white sugar", "brown sugar"],
    "atta": ["chakki atta", "ashrafi atta", "sunridge atta"],
    "bat": ["cricket bat", "tennis bat"],
    "ball": ["cricket ball", "tennis ball", "football size 5"],
}

# --------------------------
# Utilities
# --------------------------
def d(u: str) -> str:
    return urlparse(u).netloc.lower().replace("www.", "")

def abs_url(base: str, href: str) -> str:
    return urljoin(base, href)

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9\s]+", " ", s.lower()).strip()

def clean_price(text: str) -> Optional[float]:
    if not text: return None
    t = text.replace(",", "")
    m = re.search(r"(\d+(?:\.\d{1,2})?)", t)
    return float(m.group(1)) if m else None

# minimal polite rate limiter
_last_hit: Dict[str, float] = {}
async def rate_limit(host: str, min_gap: float = PER_HOST_GAP):
    last = _last_hit.get(host, 0.0)
    dt = time.time() - last
    if dt < min_gap:
        await asyncio.sleep(min_gap - dt)
    _last_hit[host] = time.time()

BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache", "Pragma": "no-cache",
}

# tiny in-memory TTL cache for fetched pages
_cache: Dict[str, Tuple[float, Optional[str]]] = {}
def cache_get(url: str) -> Optional[str]:
    v = _cache.get(url)
    if not v: return None
    ts, body = v
    if time.time() - ts > TTL_SECONDS:
        _cache.pop(url, None); return None
    return body

def cache_put(url: str, body: Optional[str]):
    _cache[url] = (time.time(), body)

async def fetch_text(client: httpx.AsyncClient, url: str, referer: Optional[str] = None) -> Optional[str]:
    cached = cache_get(url)
    if cached is not None:
        return cached
    try:
        await rate_limit(urlparse(url).netloc)
        headers = dict(BROWSER_HEADERS)
        if referer: headers["Referer"] = referer
        r = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        if r.status_code != 200:
            cache_put(url, None); return None
        cache_put(url, r.text)
        return r.text
    except Exception:
        cache_put(url, None)
        return None

# Optional: JS rendering
async def fetch_js(url: str) -> Optional[str]:
    if not USE_PLAYWRIGHT:
        return None
    try:
        from playwright.async_api import async_playwright
    except Exception:
        return None
    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            page = await browser.new_page(user_agent=BROWSER_HEADERS["User-Agent"])
            await page.goto(url, wait_until="domcontentloaded", timeout=int((REQUEST_TIMEOUT+7)*1000))
            await page.wait_for_timeout(1200)
            html = await page.content()
            await browser.close()
            return html
    except Exception:
        return None

# --------------------------
# Parsing
# --------------------------
@dataclass
class Offer:
    source: str
    title: str
    price: Optional[float]
    currency: Optional[str]
    url: str
    in_stock: Optional[bool] = None

SERVICE_BLACKLIST = [
    "service", "services", "installation", "repair", "fixing", "cleaning",
    "alignment", "movers", "packers", "examination", "inspection", "consultation"
]
def looks_like_service(title: str) -> bool:
    t = title.lower()
    return any(w in t for w in SERVICE_BLACKLIST)

def keyword_match(title: str, query: str) -> bool:
    t = _norm(title); q = _norm(query)
    if not q: return True
    toks = [x for x in q.split() if x]
    hits = sum(1 for tok in toks if tok in t)
    if hits >= max(1, len(toks)//2): return True
    return fuzz.token_set_ratio(t, q) >= 80

def parse_ldjson(html: str, page_url: str) -> List[Offer]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Offer] = []
    for tag in soup.find_all("script", type="application/ld+json"):
        raw = tag.string or ""
        if not raw.strip(): continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        blocks = data if isinstance(data, list) else [data]
        for b in blocks:
            if not isinstance(b, dict): continue
            typ = b.get("@type")
            is_product = (isinstance(typ, list) and "Product" in typ) or (typ == "Product")
            if not is_product: continue
            name = (b.get("name") or "").strip()
            url = b.get("url") or page_url
            offs = b.get("offers"); arr = offs if isinstance(offs, list) else [offs] if offs else []
            for o in arr:
                if not isinstance(o, dict): continue
                price = o.get("price")
                if price is None and isinstance(o.get("priceSpecification"), dict):
                    price = o["priceSpecification"].get("price")
                currency = o.get("priceCurrency") or (o.get("priceSpecification") or {}).get("priceCurrency")
                avail = o.get("availability"); in_stock = None
                if isinstance(avail, str):
                    in_stock = "OutOfStock" not in avail and "SoldOut" not in avail
                p = clean_price(str(price) if price is not None else "")
                if name and p is not None:
                    out.append(Offer(source=d(page_url), title=name, price=p, currency=currency, url=url, in_stock=in_stock))
    return out

def css_text(soup: BeautifulSoup, sel: Optional[str]) -> Optional[str]:
    if not sel: return None
    node = soup.select_one(sel); return node.get_text(" ", strip=True) if node else None

def parse_product_fallback(html: str, page_url: str, fallback: Dict[str, str]) -> Optional[Offer]:
    soup = BeautifulSoup(html, "html.parser")
    title = css_text(soup, fallback.get("title"))
    price_text = css_text(soup, fallback.get("price"))
    price = clean_price(price_text or "")
    if title and price is not None:
        return Offer(source=d(page_url), title=title, price=price, currency=None, url=page_url)
    return None

# --------------------------
# Platform adapters
# --------------------------
def shopify_cfg(base: str) -> Dict[str, Any]:
    return {
        "search": f"{base}/search?q={{q}}",
        "suggest": f"{base}/search/suggest.json?q={{q}}&resources[type]=product&resources[limit]=10",
        "card_sel": "a[href*='/products/']",
        "product_link_patterns": [r"/products?/[-a-z0-9]+/?$"],
        "fallback": {"title": "h1.product__title, h1.product-title, h1.page-title",
                     "price": ".price-item--regular, .price .price__regular, span.price"}
    }

def woo_cfg(base: str) -> Dict[str, Any]:
    return {
        "search": f"{base}/?s={{q}}&post_type=product",
        "card_sel": "li.product a.woocommerce-LoopProduct-link, a[href*='/product/']",
        "product_link_patterns": [r"/product/[-a-z0-9]+/?$"],
        "fallback": {"title": "h1.product_title, h1.entry-title",
                     "price": "p.price, span.price, bdi"}
    }

async def shopify_links(client, store, q, max_links=MAX_LINKS_PER_STORE) -> List[str]:
    base = store["base"].rstrip("/"); cfg = shopify_cfg(base)
    links: List[str] = []
    # 1) suggest.json
    sug = await fetch_text(client, cfg["suggest"].format(q=quote_plus(q)), referer=base)
    if sug:
        try:
            data = json.loads(sug)
            prods = data.get("resources", {}).get("results", {}).get("products", [])
            for p in prods:
                u = p.get("url");  links.append(abs_url(base, u)) if u else None
        except Exception: pass
    if len(links) >= max_links: return links[:max_links]
    # 2) search page
    html = await fetch_text(client, cfg["search"].format(q=quote_plus(q)), referer=base)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select(cfg["card_sel"]):
            href = a.get("href");  links.append(abs_url(base, href)) if href else None
        for a in soup.find_all("a", href=True):  # regex fallback
            h = a["href"]
            if any(re.search(p, h) for p in cfg["product_link_patterns"]):
                links.append(abs_url(base, h))
    # dedupe same domain
    dd = d(base); out=[]; seen=set()
    for u in links:
        if d(u).endswith(dd) and u not in seen: out.append(u); seen.add(u)
    return out[:max_links]

async def woo_links(client, store, q, max_links=MAX_LINKS_PER_STORE) -> List[str]:
    base = store["base"].rstrip("/"); cfg = woo_cfg(base)
    links: List[str] = []
    html = await fetch_text(client, cfg["search"].format(q=quote_plus(q)), referer=base)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select(cfg["card_sel"]):
            href = a.get("href");  links.append(abs_url(base, href)) if href else None
        for a in soup.find_all("a", href=True):
            if re.search(r"/product/[-a-z0-9]+/?$", a["href"]):
                links.append(abs_url(base, a["href"]))
    dd = d(base); out=[]; seen=set()
    for u in links:
        if d(u).endswith(dd) and u not in seen: out.append(u); seen.add(u)
    return out[:max_links]

async def custom_links(client, store, q, max_links=MAX_LINKS_PER_STORE) -> List[str]:
    base = store["base"].rstrip("/"); search_url = store.get("search")
    links: List[str] = []
    if search_url:
        html = await fetch_text(client, search_url.format(q=quote_plus(q)), referer=base)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            card = store.get("search_card")
            if card:
                for a in soup.select(card):
                    href = a.get("href");  links.append(abs_url(base, href)) if href else None
            pats = store.get("product_link_patterns") or []
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if any(re.search(p, h) for p in pats):
                    links.append(abs_url(base, h))
    dd = d(base); out=[]; seen=set()
    for u in links:
        if d(u).endswith(dd) and u not in seen: out.append(u); seen.add(u)
    return out[:max_links]

async def search_links(client, store, q, max_links=MAX_LINKS_PER_STORE) -> List[str]:
    plat = store["platform"]
    if plat == "shopify": return await shopify_links(client, store, q, max_links)
    if plat == "woo":     return await woo_links(client, store, q, max_links)
    return await custom_links(client, store, q, max_links)

async def parse_product(client: httpx.AsyncClient, store: Dict[str, Any], url: str) -> List[Offer]:
    html = await fetch_text(client, url, referer=store["base"])
    if not html and USE_PLAYWRIGHT:
        html = await fetch_js(url)
    if not html: return []
    offers = parse_ldjson(html, url)
    if offers: return offers
    # fallback CSS
    if store["platform"] == "shopify":
        fallback = shopify_cfg(store["base"])["fallback"]
    elif store["platform"] == "woo":
        fallback = woo_cfg(store["base"])["fallback"]
    else:
        fallback = store.get("product_fallback", {})
    one = parse_product_fallback(html, url, fallback) if fallback else None
    return [one] if one else []

# --------------------------
# Query expansion (longest match)
# --------------------------
def _score_suggestion(sugg: str, query: str) -> tuple[int, int, int]:
    t = _norm(sugg); q = _norm(query)
    q_tokens = [tok for tok in q.split() if tok]
    hits = sum(1 for tok in q_tokens if tok in t)
    fuzzy = fuzz.token_set_ratio(t, q)
    return (hits, fuzzy, len(sugg))

async def harvest_shopify_suggestions(client: httpx.AsyncClient, stores: List[dict], query: str, limit_per_store: int = 10) -> List[str]:
    suggestions: List[str] = []
    for s in stores:
        if s.get("platform") != "shopify": continue
        base = s["base"].rstrip("/")
        sug_url = f"{base}/search/suggest.json?q={quote_plus(query)}&resources[type]=product&resources[limit]={limit_per_store}"
        txt = await fetch_text(client, sug_url, referer=base)
        if not txt: continue
        try:
            data = json.loads(txt)
            prods = data.get("resources", {}).get("results", {}).get("products", [])
            for p in prods:
                title = p.get("title") or p.get("handle") or ""
                if title: suggestions.append(title.strip())
        except Exception:
            continue
    # de-dupe case-insensitive
    seen=set(); uniq=[]
    for s in suggestions:
        k=s.lower()
        if k not in seen: seen.add(k); uniq.append(s)
    return uniq

async def generate_query_variants(client: httpx.AsyncClient, all_stores: List[dict], base_query: str, max_variants: int = 4) -> List[str]:
    raw = await harvest_shopify_suggestions(client, all_stores, base_query, limit_per_store=12)
    ranked = sorted(raw, key=lambda s: _score_suggestion(s, base_query), reverse=True)
    base_norm = _norm(base_query)
    variants: List[str] = []
    for title in ranked:
        if _norm(title) == base_norm: continue
        hits, fuzzy, ln = _score_suggestion(title, base_query)
        if hits >= 1 and fuzzy >= 70 and ln > len(base_query):
            variants.append(title)
        if len(variants) >= max_variants: break
    # brand hints
    for k, hints in BRAND_HINTS.items():
        if k in base_norm:
            for h in hints:
                if h not in variants: variants.append(h)
    # uniq preserve order
    seen=set(); out=[]
    for v in variants:
        if v.lower() not in seen:
            seen.add(v.lower()); out.append(v)
    return out[:6]

# --------------------------
# Aggregation
# --------------------------
def filter_and_sort(offers: List[Offer], base_query: str) -> List[Offer]:
    cleaned = []
    for o in offers:
        if not o.title: continue
        if looks_like_service(o.title): continue
        if not keyword_match(o.title, base_query): continue
        if o.currency is None: o.currency = "PKR"
        cleaned.append(o)
    # dedupe (source + normalized title) keep cheapest
    keyd = {}
    for o in cleaned:
        k = (o.source, _norm(o.title))
        if k not in keyd or (o.price or 1e18) < (keyd[k].price or 1e18):
            keyd[k] = o
    items = list(keyd.values())
    # cross-site collapse of near-identical titles → cheapest
    collapsed: List[Offer] = []
    while items:
        base = items.pop()
        group = [base]; rest=[]
        for other in items:
            if fuzz.token_sort_ratio(base.title, other.title) >= 92:
                group.append(other)
            else:
                rest.append(other)
        items = rest
        best = min(group, key=lambda x: (x.price or 1e18))
        collapsed.append(best)
    collapsed.sort(key=lambda x: ((x.price or 1e18), x.title.lower()))
    return collapsed

# --------------------------
# FastAPI
# --------------------------
app = FastAPI(title="Live Price Comparison API", version="1.0")

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/search")
async def search(
    q: str = Query(..., description="Product keyword (e.g., 'bread', 'milk', 'laptop')"),
    limit: int = Query(40, ge=1, le=100),
):
    q = q.strip()
    if not q:
        raise HTTPException(status_code=400, detail="Missing query")

    async with httpx.AsyncClient(follow_redirects=True, timeout=REQUEST_TIMEOUT) as client:
        # expand query
        variants = await generate_query_variants(client, STORES, q, max_variants=4)
        queries = [q] + variants
        # discover product links per store per query
        link_pairs: List[Tuple[Dict[str,Any], str]] = []
        for qtry in queries:
            tasks = [search_links(client, s, qtry, max_links=MAX_LINKS_PER_STORE) for s in STORES]
            link_lists = await asyncio.gather(*tasks, return_exceptions=True)
            for store, links in zip(STORES, link_lists):
                if isinstance(links, Exception) or not links: continue
                for u in links:
                    link_pairs.append((store, u))
        # de-dupe URL per store
        seen=set(); merged=[]
        for s, u in link_pairs:
            k=(s["base"], u)
            if k in seen: continue
            seen.add(k); merged.append((s,u))
        if not merged:
            return {"query": q, "variants": variants, "offers": []}

        # parse product pages
        tasks = [parse_product(client, s, url) for s, url in merged]
        parsed = await asyncio.gather(*tasks, return_exceptions=True)

    offers: List[Offer] = []
    for r in parsed:
        if isinstance(r, Exception) or not r: continue
        offers.extend(r)

    final = filter_and_sort(offers, q)[:limit]
    return {
        "query": q,
        "variants_tried": variants,
        "count": len(final),
        "offers": [
            {
                "source": o.source,
                "title": o.title,
                "price": o.price,
                "currency": o.currency,
                "url": o.url,
                "in_stock": o.in_stock,
            } for o in final
        ]
    }
