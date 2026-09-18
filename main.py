from __future__ import annotations
import os
import json
import time
import asyncio
from collections import OrderedDict

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

# MovieBox metadata-only API — metadata endpoints + cache + dashboard
# (No streaming/download endpoints by design)

API_BASE = "https://h5-api.aoneroom.com/wefeed-h5api-bff"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Referer": "https://moviebox.ph/",
    "Origin": "https://moviebox.ph",
    "X-Client-Info": '{"timezone":"Asia/Dhaka"}',
    "X-Request-Lang": "en",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
}

_state = {"client": None, "token": None, "lock": None}

class TTLCache:
    def __init__(self, max_items: int = 500, stale_grace: int = 1800):
        self._data: OrderedDict[str, tuple[float, dict]] = OrderedDict()
        self._locks: dict[str, asyncio.Lock] = {}
        self._glock = asyncio.Lock()
        self._max = max_items
        self._grace = stale_grace

    async def get_lock(self, key: str) -> asyncio.Lock:
        async with self._glock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    def drop_lock(self, key: str, lock: asyncio.Lock):
        if not lock.locked():
            self._locks.pop(key, None)

    def lookup(self, key: str) -> tuple[dict | None, str]:
        entry = self._data.get(key)
        if not entry:
            return None, "MISS"
        expires, value = entry
        now = time.monotonic()
        if now < expires:
            self._data.move_to_end(key)
            return value, "HIT"
        if now < expires + self._grace:
            return value, "STALE"
        del self._data[key]
        return None, "MISS"

    def put(self, key: str, value: dict, ttl: int):
        self._data[key] = (time.monotonic() + ttl, value)
        self._data.move_to_end(key)
        while len(self._data) > self._max:
            self._data.popitem(last=False)

CACHE: TTLCache | None = None

def _ensure():
    """Lazy init — works on serverless where lifespan may not fire."""
    global CACHE
    if _state["client"] is None:
        _state["client"] = httpx.AsyncClient(follow_redirects=True, timeout=25)
        _state["lock"] = asyncio.Lock()
    if CACHE is None:
        CACHE = TTLCache()

app = FastAPI(title="MovieBox Metadata API", version="3.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---------- upstream ----------

async def _get_token() -> str:
    if _state["token"]:
        return _state["token"]
    async with _state["lock"]:
        if _state["token"]:
            return _state["token"]
        resp = await _state["client"].get(f"{API_BASE}/home?host=moviebox.ph", headers=DEFAULT_HEADERS)
        x_user = resp.headers.get("x-user")
        if x_user:
            _state["token"] = json.loads(x_user).get("token")
        return _state["token"] or ""

async def _fetch(url: str, method: str, payload: dict | None) -> dict:
    for attempt in range(2):
        token = await _get_token()
        headers = {**DEFAULT_HEADERS}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = await _state["client"].request(method, url, headers=headers, json=payload)
        if resp.status_code in (401, 403) and attempt == 0:
            _state["token"] = None
            continue
        x_user = resp.headers.get("x-user")
        if x_user:
            _state["token"] = json.loads(x_user).get("token") or _state["token"]
        if resp.status_code != 200:
            raise HTTPException(502, f"Upstream API error: {resp.status_code}")
        return resp.json()
    raise HTTPException(502, "Upstream auth failed")

def _cache_key(url: str, method: str, payload: dict | None) -> str:
    body = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    return f"{method}:{url}:{body}"

async def _request(url: str, method: str = "GET", payload: dict | None = None, ttl: int | None = None) -> dict:
    _ensure()
    key = _cache_key(url, method, payload)
    value, status = CACHE.lookup(key)
    if status == "HIT":
        return value
    lock = await CACHE.get_lock(key)
    async with lock:
        value, status = CACHE.lookup(key)
        if status == "HIT":
            return value
        try:
            fresh = await _fetch(url, method, payload)
        except Exception:
            if status == "STALE":
                return value
            raise
        finally:
            CACHE.drop_lock(key, lock)
        if ttl:
            CACHE.put(key, fresh, ttl)
        return fresh

def _norm(sub: dict) -> dict:
    return {
        "name": sub.get("title"),
        "poster_url": (sub.get("cover") or {}).get("url"),
        "slug": sub.get("detailPath"),
        "subject_id": sub.get("subjectId"),
        "badge": sub.get("corner"),
        "rating": sub.get("imdbRatingValue"),
        "year": (sub.get("releaseDate") or "")[:4] or None,
    }

# ---------- dashboard (original UI) ----------

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MovieBox Metadata API | Pro Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
        <style>
            :root { --primary:#ff3d71; --secondary:#3366ff; --accent:#00f2ff; --bg:#07080c; --card-bg:rgba(255,255,255,0.03); --glass:rgba(255,255,255,0.06); --text:#fff; }
            * { margin:0; padding:0; box-sizing:border-box; }
            body { font-family:'Outfit',sans-serif; background:var(--bg); color:var(--text); overflow-x:hidden; min-height:100vh;
                   background-image: radial-gradient(circle at 10% 10%, rgba(255,61,113,0.12) 0%, transparent 40%),
                                     radial-gradient(circle at 90% 90%, rgba(51,102,255,0.12) 0%, transparent 40%); }
            .container { max-width:1200px; margin:0 auto; padding:60px 24px; position:relative; }
            header { text-align:center; margin-bottom:80px; animation:fadeInDown 1s ease-out; }
            @keyframes fadeInDown { from { opacity:0; transform:translateY(-30px);} to { opacity:1; transform:translateY(0);} }
            h1 { font-size:clamp(2.5rem,8vw,4rem); font-weight:800; background:linear-gradient(135deg,#fff 0%,#aaa 100%);
                 -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin-bottom:15px; letter-spacing:-2px; }
            .badge { background:linear-gradient(90deg,var(--primary),var(--secondary)); padding:8px 18px; border-radius:40px;
                     font-size:0.85rem; font-weight:700; display:inline-block; margin-bottom:25px; text-transform:uppercase;
                     letter-spacing:1px; box-shadow:0 10px 30px rgba(255,61,113,0.3); }
            .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(340px,1fr)); gap:30px; margin-top:20px; }
            .card { background:var(--card-bg); border:1px solid var(--glass); border-radius:28px; padding:35px;
                    transition:all 0.4s cubic-bezier(0.175,0.885,0.32,1.275); backdrop-filter:blur(12px);
                    position:relative; overflow:hidden; display:flex; flex-direction:column; }
            @media (hover: hover) { .card:hover { transform:translateY(-12px) scale(1.02); border-color:rgba(255,255,255,0.2); box-shadow:0 30px 60px rgba(0,0,0,0.5); } }
            .card-title { font-size:1.5rem; font-weight:700; margin-bottom:18px; display:flex; align-items:center; gap:12px; }
            .card-title i { width:32px; height:32px; background:rgba(255,255,255,0.05); border-radius:8px; display:flex;
                            align-items:center; justify-content:center; font-size:1rem; color:var(--accent); font-style:normal; }
            .card-desc { color:#9ea3ac; font-size:1rem; line-height:1.6; margin-bottom:25px; flex-grow:1; }
            .endpoint { font-family:'JetBrains Mono',monospace; background:rgba(0,0,0,0.4); padding:14px; border-radius:14px;
                        font-size:0.85rem; color:var(--accent); border:1px solid rgba(0,242,255,0.15); margin-bottom:25px;
                        word-break:break-all; position:relative; }
            .endpoint::after { content:'GET'; position:absolute; right:14px; top:14px; font-size:0.65rem; font-weight:800; color:rgba(255,255,255,0.3); }
            .btn { display:flex; align-items:center; justify-content:center; padding:16px; background:#fff; color:#000;
                   text-decoration:none; border-radius:16px; font-weight:700; font-size:0.95rem; transition:all 0.3s; }
            .btn:hover { background:var(--primary); color:#fff; transform:translateY(-2px); box-shadow:0 10px 25px rgba(255,61,113,0.4); }
            footer { text-align:center; padding:80px 0 40px; animation:fadeIn 2s ease; }
            @keyframes fadeIn { from { opacity:0;} to { opacity:1;} }
            .dev-tag { font-weight:800; color:#666; letter-spacing:3px; text-transform:uppercase; font-size:0.75rem;
                       border:1px solid #222; padding:12px 30px; border-radius:50px; display:inline-block;
                       background:rgba(255,255,255,0.01); transition:all 0.3s; }
            .dev-tag:hover { color:var(--text); border-color:var(--primary); letter-spacing:5px; }
            @media (max-width:480px) { .container { padding:40px 16px; } .card { padding:25px; } h1 { margin-bottom:10px; } }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div class="badge">Metadata API</div>
                <h1>MovieBox Pro</h1>
                <p style="color:#667; font-size:1.25rem; font-weight:300;">State-of-the-Art Pure API Architecture</p>
            </header>
            <div class="grid">
                <div class="card">
                    <div class="card-title"><i>🏠</i> Discover Home</div>
                    <p class="card-desc">The ultimate window into MovieBox. Headlines, recommended content, and trending blocks updated in real-time.</p>
                    <div class="endpoint">/home</div>
                    <a href="/home" target="_blank" class="btn">Launch API</a>
                </div>
                <div class="card">
                    <div class="card-title"><i>🔍</i> Smart Search</div>
                    <p class="card-desc">High-precision search engine results. Returns titles, posters, and slugs for lightning-fast matching.</p>
                    <div class="endpoint">/search?q=Attack on Titan</div>
                    <a href="/search?q=Attack%20on%20Titan" target="_blank" class="btn">Test Search</a>
                </div>
                <div class="card">
                    <div class="card-title"><i>🆔</i> Metadata A-Z</div>
                    <p class="card-desc">Deep-dive into any subject. Episodes, seasons, languages, and full high-resolution metadata trees.</p>
                    <div class="endpoint">/detail/{slug}</div>
                    <a href="/detail/attack-on-titan-hindi-kGWQOIx0d4" target="_blank" class="btn">Fetch Specs</a>
                </div>
                <div class="card">
                    <div class="card-title"><i>📦</i> Catalog Filters</div>
                    <p class="card-desc">Paginated collections for all genres. Movies, TV shows, and Animations filtered by professional criteria. Pagination Supported.</p>
                    <div class="endpoint">/tv-series?page=2</div>
                    <a href="/tv-series?page=2" target="_blank" class="btn">Test Page 2</a>
                </div>
                <div class="card">
                    <div class="card-title"><i>⚡</i> Cached Responses</div>
                    <p class="card-desc">Every endpoint is served through a TTL cache with single-flight locking — fast repeat responses, zero upstream hammering.</p>
                    <div class="endpoint">/docs</div>
                    <a href="/docs" target="_blank" class="btn">Open Swagger</a>
                </div>
            </div>
            <footer>
                <div class="dev-tag">Developer: Walter</div>
            </footer>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# ---------- routes ----------

@app.get("/home")
async def get_home():
    data = await _request(f"{API_BASE}/home?host=moviebox.ph", ttl=300)
    sections = []
    for op in (data.get("data") or {}).get("operatingList") or []:
        if op.get("type") == "BANNER":
            items = [_norm(i.get("subject") or {}) for i in op.get("banner", {}).get("items", []) if i.get("subject")]
            sections.append({"section": "Banner", "count": len(items), "items": items})
        elif op.get("type") in ("SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"):
            items = [_norm(s) for s in op.get("subjects", [])]
            sections.append({"section": op.get("title", "Featured"), "count": len(items), "items": items})
    return {"status": "success", "sections": sections}

async def _category(tab_id: int, page: int, per_page: int, sort: str) -> dict:
    data = await _request(f"{API_BASE}/subject/filter", method="POST", payload={
        "tabId": tab_id,
        "filter": {"sort": sort, "genre": "ALL", "country": "ALL", "year": "ALL", "language": "ALL"},
        "page": page, "perPage": per_page,
    }, ttl=600)
    inner = data.get("data", {})
    items = [_norm(s) for s in inner.get("items", inner.get("subjects", []))]
    pager = inner.get("pager", {})
    return {"page": page, "per_page": per_page, "total": pager.get("totalCount") or len(items), "items": items}

@app.get("/movies")
async def movies(page: int = 1, sort: str = "RECOMMEND"):
    return await _category(2, page, 24, sort)

@app.get("/tv-series")
async def tv_series(page: int = 1, sort: str = "RECOMMEND"):
    return await _category(5, page, 24, sort)

@app.get("/animation")
async def animation(page: int = 1, sort: str = "RECOMMEND"):
    return await _category(8, page, 24, sort)

@app.get("/search/suggest")
async def search_suggest(q: str = Query(..., min_length=1)):
    data = await _request(f"{API_BASE}/subject/search-suggest", method="POST",
                          payload={"keyword": q, "perPage": 10}, ttl=120)
    raw = (data.get("data") or {}).get("items", [])
    return {"suggestions": [_norm(i.get("subject") or {}) for i in raw if i.get("subject")]}

@app.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = 1):
    data = await _request(f"{API_BASE}/subject/search", method="POST",
                          payload={"keyword": q, "page": page, "perPage": 20}, ttl=300)
    inner = data.get("data", {})
    items = [_norm(s) for s in inner.get("items", inner.get("list", []))]
    pager = inner.get("pager", {})
    return {"query": q, "page": page, "total": pager.get("totalCount") or len(items), "items": items}

@app.get("/detail/{slug}")
async def detail(slug: str):
    data = await _request(f"{API_BASE}/detail?detailPath={slug}", ttl=3600)
    return data.get("data", {})
          
