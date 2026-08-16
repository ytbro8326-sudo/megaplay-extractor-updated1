#!/usr/bin/env python3
"""
AniZone High-Performance Stream Extractor & Web Player
Production-Ready Web Service (FastAPI + Uvicorn)
Supports dynamic M3U8 generation, multi-audio (English Dub / Japanese Sub) synchronization,
MAL ID resolution, title search, streaming proxy, and cloud deployment.
"""

import os
import re
import sys
import json
import html
import random
import urllib.parse
import urllib.request
import urllib.error
from typing import Optional, List, Dict, Any

import requests
import urllib3
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

urllib3.disable_warnings()

# ─────────────────────────────────────────────────────────────
# CONSTANTS & CONFIGURATION
# ─────────────────────────────────────────────────────────────

CDN_BASE = "https://seiryuu.vid-cdn.xyz"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://anizone.to/",
}

PROXIES = [
    "http://dxicdysy:yndikr9coeto@31.59.20.176:6754",
    "http://dxicdysy:yndikr9coeto@31.56.127.193:7684",
    "http://dxicdysy:yndikr9coeto@45.38.107.97:6014",
    "http://dxicdysy:yndikr9coeto@198.105.121.200:6462",
    "http://dxicdysy:yndikr9coeto@64.137.96.74:6641",
    "http://dxicdysy:yndikr9coeto@198.23.243.226:6361",
    "http://dxicdysy:yndikr9coeto@38.154.185.97:6370",
    "http://dxicdysy:yndikr9coeto@84.247.60.125:6095",
    "http://dxicdysy:yndikr9coeto@142.111.67.146:5611",
    "http://dxicdysy:yndikr9coeto@191.96.254.138:6185"
]

SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update(DEFAULT_HEADERS)

app = FastAPI(
    title="AniZone Multi-Audio Stream Extractor & Web Player",
    description="High-performance stream extractor and synchronous multi-audio player for AniZone.to",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# UTILITIES & FETCHING
# ─────────────────────────────────────────────────────────────

def get_proxy_opener(proxy_url: str):
    proxy_handler = urllib.request.ProxyHandler({
        "http": proxy_url,
        "https": proxy_url
    })
    return urllib.request.build_opener(proxy_handler)


def fetch(url: str, extra_headers: Optional[dict] = None) -> str:
    """Fetch URL with direct request first, falling back to proxy rotation."""
    headers = {**DEFAULT_HEADERS, **(extra_headers or {})}
    
    # 1. Direct attempt
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=12) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        pass

    # 2. Proxy pool attempt
    shuffled_proxies = list(PROXIES)
    random.shuffle(shuffled_proxies)
    for proxy in shuffled_proxies:
        try:
            opener = get_proxy_opener(proxy)
            req = urllib.request.Request(url, headers=headers)
            with opener.open(req, timeout=10) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception:
            continue

    raise Exception(f"Failed to fetch content from {url}")


def extract_uuid(text: str) -> Optional[str]:
    """Extract 36-char video UUID from URL, HTML, or raw string."""
    patterns = [
        r"https?://[^/]+/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        r"vid-cdn\.xyz/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).lower()
    return None


def parse_master_m3u8(master_content: str, master_url: str) -> dict:
    """Parse master playlist to extract video quality tracks and audio renditions."""
    base = master_url.rsplit("/", 1)[0]
    streams = {"videos": [], "audio": []}

    lines = master_content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("#EXT-X-STREAM-INF"):
            res = re.search(r'RESOLUTION=([\dx]+)', line)
            bw = re.search(r'BANDWIDTH=(\d+)', line)
            resolution = res.group(1) if res else "Unknown"
            bitrate = int(bw.group(1)) // 1000 if bw else 0
            label = f"{resolution} ({bitrate} kbps)" if bitrate else resolution

            if i + 1 < len(lines):
                url = lines[i + 1].strip()
                if url and not url.startswith("#"):
                    if not url.startswith("http"):
                        url = base + "/" + url
                    streams["videos"].append({
                        "label": label,
                        "resolution": resolution,
                        "bitrate": bitrate,
                        "url": url
                    })
            i += 2
            continue

        if line.startswith("#EXT-X-MEDIA") and "TYPE=AUDIO" in line:
            name_m = re.search(r'NAME="?([^",]+)"?', line)
            lang_m = re.search(r'LANGUAGE="?([^",\s]+)"?', line)
            uri_m = re.search(r'URI="?([^",\s]+)"?', line)
            default_m = re.search(r'DEFAULT=(YES|NO)', line, re.IGNORECASE)

            name = name_m.group(1) if name_m else "Audio"
            lang = lang_m.group(1).lower() if lang_m else "?"
            is_default = (default_m.group(1).upper() == "YES") if default_m else False
            u = uri_m.group(1) if uri_m else ""
            if u and not u.startswith("http"):
                u = base + "/" + u

            streams["audio"].append({
                "name": name,
                "lang": lang,
                "isDefault": is_default,
                "url": u
            })

        i += 1

    return streams


def rewrite_m3u8_playlist(content: str, playlist_url: str, proxy_base: str) -> str:
    """
    Rewrites any M3U8 playlist (master or variant) so that all child resources
    (AES keys, media audio tracks, variant playlists, and .ts segment chunks)
    are routed through the CORS reverse proxy.
    """
    base_folder = playlist_url.rsplit("/", 1)[0] + "/"
    out_lines = []

    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        # 1. Decryption Keys (#EXT-X-KEY)
        if stripped.startswith("#EXT-X-KEY"):
            def repl_key(m):
                key_uri = m.group(1)
                abs_key = key_uri if key_uri.startswith("http") else urllib.parse.urljoin(base_folder, key_uri)
                proxied_key = f"{proxy_base}?url={urllib.parse.quote(abs_key, safe='')}"
                return f'URI="{proxied_key}"'
            line = re.sub(r'URI="([^"]+)"', repl_key, line)
            out_lines.append(line)
            continue

        # 2. Audio & Subtitle Media Tracks (#EXT-X-MEDIA)
        if stripped.startswith("#EXT-X-MEDIA"):
            def repl_media(m):
                media_uri = m.group(1)
                abs_media = media_uri if media_uri.startswith("http") else urllib.parse.urljoin(base_folder, media_uri)
                proxied_media = f"{proxy_base}?url={urllib.parse.quote(abs_media, safe='')}"
                return f'URI="{proxied_media}"'
            line = re.sub(r'URI="([^"]+)"', repl_media, line)
            out_lines.append(line)
            continue

        # 3. Other M3U8 tags (#EXT...)
        if stripped.startswith("#"):
            out_lines.append(line)
            continue

        # 4. Stream URIs & .ts Segment files
        abs_seg = stripped if stripped.startswith("http") else urllib.parse.urljoin(base_folder, stripped)
        proxied_seg = f"{proxy_base}?url={urllib.parse.quote(abs_seg, safe='')}"
        out_lines.append(proxied_seg)

    return "\n".join(out_lines)


def patch_master_m3u8(master_content: str, master_url: str, default_audio: str = "en", proxy_base: str = "") -> str:
    """
    Patches master.m3u8 content so that:
    1. The selected audio language (default: 'en' for English Dub) has DEFAULT=YES & AUTOSELECT=YES.
    2. Non-selected audio tracks have DEFAULT=NO & AUTOSELECT=NO.
    3. All child variant playlists, audio tracks, and segments route through the reverse proxy.
    """
    target_lang = default_audio.lower().strip()
    lines_with_defaults = []

    for line in master_content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith("#EXT-X-MEDIA") and "TYPE=AUDIO" in line:
            lang_match = re.search(r'LANGUAGE="?([^",\s]+)"?', line, re.IGNORECASE)
            name_match = re.search(r'NAME="?([^",]+)"?', line, re.IGNORECASE)
            
            lang = (lang_match.group(1) if lang_match else "").lower()
            name = (name_match.group(1) if name_match else "").lower()
            
            is_target = False
            if target_lang in ("en", "eng", "english", "dub"):
                is_target = lang in ("en", "eng") or "english" in name or "dub" in name
            elif target_lang in ("ja", "jp", "japanese", "sub", "jpn"):
                is_target = lang in ("ja", "jp", "jpn") or "japanese" in name or "sub" in name
            elif target_lang in ("all", "both"):
                is_target = lang in ("en", "eng")

            def_val = "YES" if is_target else "NO"

            if "DEFAULT=" in line:
                line = re.sub(r'DEFAULT=(YES|NO)', f'DEFAULT={def_val}', line, flags=re.IGNORECASE)
            else:
                line += f',DEFAULT={def_val}'

            if "AUTOSELECT=" in line:
                line = re.sub(r'AUTOSELECT=(YES|NO)', f'AUTOSELECT={def_val}', line, flags=re.IGNORECASE)
            else:
                line += f',AUTOSELECT={def_val}'

        lines_with_defaults.append(line)

    content_with_defaults = "\n".join(lines_with_defaults)
    if proxy_base:
        return rewrite_m3u8_playlist(content_with_defaults, master_url, proxy_base)
    return content_with_defaults


# ─────────────────────────────────────────────────────────────
# MAL / ANILIST & ANIZONE SEARCH HELPERS
# ─────────────────────────────────────────────────────────────

def norm(s: str = "") -> str:
    return re.sub(r'[^\w]', '', (s or "").lower())


def dice_coeff(a: str, b: str) -> float:
    ca, cb = norm(a), norm(b)
    if not ca or not cb:
        return 0.0
    if ca == cb:
        return 1.0
    bg_a = {ca[i:i+2] for i in range(len(ca)-1)} or {ca}
    bg_b = {cb[i:i+2] for i in range(len(cb)-1)} or {cb}
    inter = len(bg_a & bg_b)
    return (2.0 * inter) / (len(bg_a) + len(bg_b))


def search_anizone_titles(query: str) -> List[dict]:
    url = f"https://anizone.to/anime?search={urllib.parse.quote(query)}"
    try:
        html_str = fetch(url)
    except Exception:
        return []
    results = []
    seen = set()

    for m in re.finditer(r'x-data="(\{[^"]*anmTitles[^"]*\})"', html_str):
        idx = m.start()
        ctx = html_str[max(0, idx - 300):min(len(html_str), idx + len(m.group(0)) + 800)]
        slug_m = re.search(r'href="(?:https://anizone\.to)?/anime/([a-z0-9-]+)"', ctx)
        if not slug_m:
            continue
        slug = slug_m.group(1)
        if slug in seen:
            continue

        raw_xdata = html.unescape(m.group(1)).strip()
        json_m = re.search(r'anmTitles:\s*JSON\.parse\(\'((?:[^\'\\]|\\.)*)\'\)', raw_xdata)
        if not json_m:
            continue
        try:
            unescaped_json = json_m.group(1).encode().decode('unicode_escape')
            titles_dict = json.loads(unescaped_json)
            title = titles_dict.get("1") or titles_dict.get("5") or titles_dict.get("8") or (list(titles_dict.values())[0] if titles_dict else slug)
            results.append({"slug": slug, "title": title, "url": f"https://anizone.to/anime/{slug}"})
            seen.add(slug)
        except Exception:
            continue

    return results


def get_anilist_media_by_mal_id(mal_id: int) -> dict:
    query = """
    query ($idMal: Int) {
      Media (idMal: $idMal, type: ANIME) {
        id
        idMal
        title { english romaji native }
        status
        episodes
        seasonYear
        synonyms
        coverImage { large medium }
      }
    }
    """
    req_data = json.dumps({"query": query, "variables": {"idMal": int(mal_id)}}).encode("utf-8")
    headers = {"Content-Type": "application/json", "User-Agent": DEFAULT_HEADERS["User-Agent"]}
    req = urllib.request.Request("https://graphql.anilist.co", data=req_data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=12) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        media = data.get("data", {}).get("Media")
        if not media:
            raise Exception(f"No anime found on AniList for MAL ID {mal_id}")
        return media


# ─────────────────────────────────────────────────────────────
# CORE EXTRACTION LOGIC
# ─────────────────────────────────────────────────────────────

def extract_anizone_streams(target: str, request: Request) -> dict:
    """
    Extracts stream information from either:
    - Direct AniZone page URL (e.g. https://anizone.to/anime/spjuxray/3)
    - Video UUID (e.g. 8d3f2c88-e5a6-4806-8f89-14f8e09b5238)
    """
    uuid = extract_uuid(target)
    page_title = "AniZone Stream"
    page_url = target if target.startswith("http") else None

    if not uuid:
        # Fetch page to discover UUID
        if not target.startswith("http"):
            target = f"https://anizone.to/anime/{target}"
            page_url = target
        html_content = fetch(target)
        uuid = extract_uuid(html_content)
        if not uuid:
            raise HTTPException(status_code=404, detail="Could not extract video UUID from AniZone page.")

        title_m = re.search(r'<title>([^<]+)</title>', html_content)
        if title_m:
            page_title = title_m.group(1).replace("Watch", "").replace("Online Free", "").replace("AniZone", "").strip(" -|")

    master_url = f"{CDN_BASE}/{uuid}/master.m3u8"
    master_content = fetch(master_url, extra_headers={"Origin": "https://anizone.to"})
    streams = parse_master_m3u8(master_content, master_url)

    base_host = str(request.base_url).rstrip("/")
    english_master = f"{base_host}/playlist/{uuid}/master.m3u8?audio=en"
    japanese_master = f"{base_host}/playlist/{uuid}/master.m3u8?audio=ja"
    both_master = f"{base_host}/playlist/{uuid}/master.m3u8?audio=both"

    return {
        "status": "success",
        "title": page_title,
        "uuid": uuid,
        "pageUrl": page_url,
        "masterPlaylists": {
            "englishDubDefault": english_master,
            "japaneseSubDefault": japanese_master,
            "originalCdnMaster": master_url,
            "multiAudioMaster": both_master
        },
        "videoQualities": streams["videos"],
        "audioTracks": streams["audio"],
        "commands": {
            "mpv_english": f'mpv --alang=en "{english_master}"',
            "mpv_japanese": f'mpv --alang=ja "{japanese_master}"',
            "vlc": f'vlc "{english_master}"',
            "ffmpeg_english_mp4": f'ffmpeg -i "{english_master}" -c copy "{uuid}_english.mp4"'
        }
    }


# ─────────────────────────────────────────────────────────────
# REST API ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.get("/api/extract")
async def api_extract(request: Request, url: str = Query(..., description="AniZone page URL or UUID")):
    try:
        data = extract_anizone_streams(url, request)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/mal/{mal_id}/{episode}")
async def api_extract_mal(request: Request, mal_id: int, episode: int):
    try:
        media = get_anilist_media_by_mal_id(mal_id)
        titles = []
        if media.get("title"):
            for k in ["english", "romaji", "native"]:
                if media["title"].get(k):
                    titles.append(media["title"][k])
        if media.get("synonyms"):
            titles.extend(media["synonyms"])
        titles = list(dict.fromkeys(titles))

        candidates = {}
        for t in titles[:3]:
            res = search_anizone_titles(t)
            for r in res:
                if r["slug"] not in candidates:
                    candidates[r["slug"]] = r["title"]

        scored = []
        for slug, text in candidates.items():
            best = max(dice_coeff(t, text) for t in titles[:2])
            if best >= 0.35:
                scored.append({"slug": slug, "title": text, "score": best})

        scored.sort(key=lambda x: x["score"], reverse=True)
        if not scored:
            raise HTTPException(status_code=404, detail=f"No matching anime found on AniZone for MAL ID {mal_id}")

        matched_slug = scored[0]["slug"]
        page_url = f"https://anizone.to/anime/{matched_slug}/{episode}"
        data = extract_anizone_streams(page_url, request)
        data["malId"] = mal_id
        data["episode"] = episode
        data["matchedAnime"] = scored[0]["title"]
        data["coverImage"] = media.get("coverImage", {}).get("large")
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search")
async def api_search(q: str = Query(..., min_length=1)):
    try:
        results = search_anizone_titles(q)
        return {"query": q, "count": len(results), "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/playlist/{uuid}/master.m3u8")
@app.options("/playlist/{uuid}/master.m3u8")
async def dynamic_master_playlist(
    request: Request,
    uuid: str,
    audio: str = Query("en", description="Default audio track: 'en' for English Dub, 'ja' for Japanese Sub")
):
    """
    Dynamically serves an RFC 8216 compliant HLS Master Playlist with English Dub (or Japanese Sub)
    configured as DEFAULT=YES and AUTOSELECT=YES, with all child variant playlists, audio tracks,
    AES keys, and TS segments automatically routed through the reverse proxy with 100% CORS support.
    """
    if request.method == "OPTIONS":
        return Response(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "*",
            }
        )

    clean_uuid = extract_uuid(uuid)
    if not clean_uuid:
        raise HTTPException(status_code=400, detail="Invalid video UUID format")

    master_url = f"{CDN_BASE}/{clean_uuid}/master.m3u8"
    try:
        raw_master = fetch(master_url, extra_headers={"Origin": "https://anizone.to"})
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch upstream master playlist: {e}")

    base_host = str(request.base_url).rstrip("/")
    proxy_base = f"{base_host}/proxy"
    patched = patch_master_m3u8(raw_master, master_url, default_audio=audio, proxy_base=proxy_base)

    return Response(
        content=patched,
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Cache-Control": "public, max-age=3600",
            "Content-Disposition": f'inline; filename="{clean_uuid}_{audio}_master.m3u8"'
        }
    )


@app.get("/proxy")
@app.head("/proxy")
@app.options("/proxy")
async def proxy_stream(request: Request, url: str = Query("")):
    """Universal CORS streaming proxy with proper Referer spoofing and recursive M3U8 rewriting."""
    if request.method == "OPTIONS":
        return Response(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "*",
            }
        )

    if not url:
        raise HTTPException(status_code=400, detail="Missing url parameter")

    headers = {
        "User-Agent": DEFAULT_HEADERS["User-Agent"],
        "Referer": "https://anizone.to/",
        "Origin": "https://anizone.to"
    }

    try:
        upstream = SESSION.get(url, headers=headers, timeout=25, stream=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Proxy connection failed: {e}")

    content_type = upstream.headers.get("Content-Type", "application/octet-stream")
    is_m3u8 = (
        "mpegurl" in content_type.lower()
        or url.split("?")[0].lower().endswith(".m3u8")
    )

    if is_m3u8:
        raw_text = upstream.content.decode("utf-8", errors="replace")
        base_host = str(request.base_url).rstrip("/")
        proxy_base = f"{base_host}/proxy"
        rewritten_m3u8 = rewrite_m3u8_playlist(raw_text, url, proxy_base)
        return Response(
            content=rewritten_m3u8,
            status_code=upstream.status_code,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Cache-Control": "public, max-age=3600",
            }
        )

    def stream_chunks():
        for chunk in upstream.iter_content(chunk_size=65536):
            if chunk:
                yield chunk

    return StreamingResponse(
        stream_chunks(),
        status_code=upstream.status_code,
        media_type=content_type,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Cache-Control": "public, max-age=86400"
        }
    )


# ─────────────────────────────────────────────────────────────
# WEB APPLICATION & EMBEDDED PLAYER (HTML/CSS/JS)
# ─────────────────────────────────────────────────────────────

HTML_UI = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>AniZone Pro Extractor & Multi-Audio Player</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <style>
    :root {
      --bg: #090b10;
      --card-bg: rgba(17, 24, 39, 0.7);
      --card-border: rgba(255, 255, 255, 0.08);
      --accent-1: #8b5cf6;
      --accent-2: #ec4899;
      --accent-gradient: linear-gradient(135deg, #8b5cf6 0%, #ec4899 100%);
      --accent-glow: rgba(139, 92, 246, 0.35);
      --text: #f9fafb;
      --subtext: #9ca3af;
      --code-bg: #0d1117;
      --success: #10b981;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background-color: var(--bg);
      background-image: 
        radial-gradient(at 0% 0%, rgba(139, 92, 246, 0.15) 0px, transparent 50%),
        radial-gradient(at 100% 100%, rgba(236, 72, 153, 0.12) 0px, transparent 50%);
      color: var(--text);
      font-family: 'Inter', system-ui, -apple-system, sans-serif;
      min-height: 100vh;
      padding: 30px 16px;
    }

    .container {
      max-width: 1080px;
      margin: 0 auto;
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    header {
      text-align: center;
      margin-bottom: 6px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 14px;
      background: rgba(139, 92, 246, 0.15);
      border: 1px solid rgba(139, 92, 246, 0.35);
      color: #c4b5fd;
      border-radius: 9999px;
      font-size: 0.8rem;
      font-weight: 600;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      margin-bottom: 12px;
    }

    .badge-dot {
      width: 8px;
      height: 8px;
      background: #10b981;
      border-radius: 50%;
      box-shadow: 0 0 8px #10b981;
    }

    header h1 {
      font-size: 2.3rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      background: var(--accent-gradient);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      margin-bottom: 6px;
    }

    header p { color: var(--subtext); font-size: 0.95rem; }

    .card {
      background: var(--card-bg);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 24px;
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
    }

    .nav-tabs {
      display: flex;
      gap: 10px;
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 12px;
      margin-bottom: 18px;
    }

    .nav-tab {
      background: transparent;
      border: none;
      color: var(--subtext);
      font-size: 0.9rem;
      font-weight: 600;
      padding: 8px 16px;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.2s;
    }

    .nav-tab.active {
      background: rgba(139, 92, 246, 0.18);
      color: #fff;
      border: 1px solid rgba(139, 92, 246, 0.4);
    }

    .tab-pane { display: none; }
    .tab-pane.active { display: block; }

    .input-row {
      display: flex;
      gap: 12px;
      align-items: flex-end;
    }

    @media (max-width: 680px) {
      .input-row { flex-direction: column; align-items: stretch; }
    }

    .form-group {
      display: flex;
      flex-direction: column;
      gap: 6px;
      flex: 1;
    }

    .form-group label {
      font-size: 0.82rem;
      font-weight: 600;
      color: #d1d5db;
    }

    input {
      background: rgba(13, 17, 23, 0.8);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 12px 16px;
      color: #fff;
      font-size: 0.95rem;
      outline: none;
      transition: all 0.2s;
    }

    input:focus {
      border-color: #8b5cf6;
      box-shadow: 0 0 0 3px rgba(139, 92, 246, 0.25);
    }

    .btn-primary {
      background: var(--accent-gradient);
      color: white;
      border: none;
      border-radius: 10px;
      padding: 12px 24px;
      font-size: 0.95rem;
      font-weight: 600;
      cursor: pointer;
      height: 46px;
      box-shadow: 0 4px 15px var(--accent-glow);
      transition: transform 0.15s, opacity 0.15s;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      white-space: nowrap;
    }

    .btn-primary:hover { opacity: 0.92; transform: translateY(-1px); }
    .btn-primary:active { transform: translateY(0); }

    .presets {
      display: flex;
      gap: 8px;
      margin-top: 14px;
      flex-wrap: wrap;
      align-items: center;
    }

    .preset-title { font-size: 0.78rem; color: var(--subtext); margin-right: 4px; }

    .chip {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--card-border);
      color: #e5e7eb;
      padding: 5px 12px;
      border-radius: 20px;
      font-size: 0.75rem;
      cursor: pointer;
      transition: all 0.2s;
    }

    .chip:hover {
      background: rgba(139, 92, 246, 0.25);
      border-color: #8b5cf6;
    }

    /* Player Container */
    .player-card {
      display: none;
      background: #000;
      border-radius: 14px;
      overflow: hidden;
      border: 1px solid var(--card-border);
      box-shadow: 0 20px 50px rgba(0, 0, 0, 0.7);
    }

    .player-header {
      padding: 12px 18px;
      background: #111827;
      border-bottom: 1px solid var(--card-border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 10px;
    }

    .player-title {
      font-size: 1rem;
      font-weight: 700;
      color: #fff;
    }

    .player-badge {
      font-size: 0.75rem;
      padding: 3px 8px;
      border-radius: 6px;
      background: rgba(16, 185, 129, 0.15);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.3);
      font-weight: 600;
    }

    video {
      width: 100%;
      max-height: 540px;
      display: block;
      background: #000;
    }

    .player-controls-bar {
      padding: 14px 18px;
      background: #111827;
      border-top: 1px solid var(--card-border);
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
    }

    .control-group {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .control-label {
      font-size: 0.75rem;
      font-weight: 700;
      color: #9ca3af;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .btn-toggle {
      background: #1f2937;
      border: 1px solid var(--card-border);
      color: #d1d5db;
      padding: 6px 12px;
      border-radius: 8px;
      font-size: 0.8rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s;
    }

    .btn-toggle:hover {
      background: #374151;
      color: #fff;
    }

    .btn-toggle.active {
      background: var(--accent-gradient);
      border-color: transparent;
      color: #fff;
      box-shadow: 0 2px 10px var(--accent-glow);
    }

    /* Streams & Links Section */
    .links-card { display: none; }

    .link-row {
      background: var(--code-bg);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 12px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }

    .link-info {
      display: flex;
      flex-direction: column;
      gap: 4px;
      overflow: hidden;
    }

    .link-name {
      font-size: 0.85rem;
      font-weight: 700;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .link-url {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.78rem;
      color: #34d399;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .link-actions {
      display: flex;
      gap: 8px;
      flex-shrink: 0;
    }

    .btn-sm {
      background: rgba(255, 255, 255, 0.08);
      border: none;
      color: #fff;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 0.75rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s;
    }

    .btn-sm:hover { background: rgba(255, 255, 255, 0.2); }

    .btn-play-sm {
      background: var(--accent-gradient);
      box-shadow: 0 2px 8px var(--accent-glow);
    }

    pre {
      background: var(--code-bg);
      padding: 16px;
      border-radius: 10px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.8rem;
      color: #93c5fd;
      overflow-x: auto;
      max-height: 380px;
    }

    .toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: var(--success);
      color: white;
      padding: 10px 18px;
      border-radius: 8px;
      font-size: 0.85rem;
      font-weight: 600;
      display: none;
      box-shadow: 0 10px 20px rgba(0,0,0,0.3);
      z-index: 1000;
    }

    .loader {
      width: 18px;
      height: 18px;
      border: 2px solid #ffffff;
      border-bottom-color: transparent;
      border-radius: 50%;
      display: inline-block;
      animation: spin 1s linear infinite;
    }

    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="badge">
        <span class="badge-dot"></span>
        AniZone Sync Multi-Audio Engine v2.0
      </div>
      <h1>AniZone Stream Extractor & Web Player</h1>
      <p>Direct HLS stream extraction with synchronized English Dub & Japanese Sub audio tracks</p>
    </header>

    <div class="card">
      <div class="nav-tabs">
        <button class="nav-tab active" onclick="switchNav('tab-url', this)">🔗 Direct URL / UUID</button>
        <button class="nav-tab" onclick="switchNav('tab-mal', this)">📺 MAL ID & Episode</button>
        <button class="nav-tab" onclick="switchNav('tab-search', this)">🔍 Search Anime</button>
      </div>

      <!-- Tab 1: Direct URL -->
      <div class="tab-pane active" id="tab-url">
        <div class="input-row">
          <div class="form-group">
            <label for="directUrl">AniZone Episode URL or Video UUID</label>
            <input type="text" id="directUrl" value="https://anizone.to/anime/spjuxray/3" placeholder="https://anizone.to/anime/... or 8d3f2c88-...">
          </div>
          <button class="btn-primary" id="btnExtractUrl" onclick="extractByUrl()">
            <span class="btn-lbl">Extract & Play Stream</span>
            <span class="loader" style="display:none;"></span>
          </button>
        </div>
        <div class="presets">
          <span class="preset-title">Presets:</span>
          <div class="chip" onclick="setUrlPreset('https://anizone.to/anime/spjuxray/3')">Demon Slayer S4 Ep 3</div>
          <div class="chip" onclick="setUrlPreset('https://anizone.to/anime/09wkn1x1/1')">Attack on Titan Ep 1</div>
          <div class="chip" onclick="setUrlPreset('https://anizone.to/anime/k04w41t3/1')">Solo Leveling Ep 1</div>
        </div>
      </div>

      <!-- Tab 2: MAL ID -->
      <div class="tab-pane" id="tab-mal">
        <div class="input-row">
          <div class="form-group">
            <label for="malId">MyAnimeList ID (MAL ID)</label>
            <input type="number" id="malId" value="1735" placeholder="e.g. 1735">
          </div>
          <div class="form-group" style="max-width: 140px;">
            <label for="epNum">Episode</label>
            <input type="number" id="epNum" value="1" placeholder="1">
          </div>
          <button class="btn-primary" id="btnExtractMal" onclick="extractByMal()">
            <span class="btn-lbl">Resolve & Play</span>
            <span class="loader" style="display:none;"></span>
          </button>
        </div>
        <div class="presets">
          <span class="preset-title">Presets:</span>
          <div class="chip" onclick="setMalPreset(1735, 1)">Naruto Shippuden Ep 1</div>
          <div class="chip" onclick="setMalPreset(21, 1)">One Piece Ep 1</div>
          <div class="chip" onclick="setMalPreset(30276, 1)">One Punch Man Ep 1</div>
          <div class="chip" onclick="setMalPreset(269, 1)">Bleach Ep 1</div>
        </div>
      </div>

      <!-- Tab 3: Search -->
      <div class="tab-pane" id="tab-search">
        <div class="input-row">
          <div class="form-group">
            <label for="searchQuery">Search Anime Title on AniZone</label>
            <input type="text" id="searchQuery" placeholder="e.g. Jujutsu Kaisen, Bleach, Naruto..." onkeydown="if(event.key==='Enter') searchAnime()">
          </div>
          <button class="btn-primary" id="btnSearch" onclick="searchAnime()">
            <span class="btn-lbl">Search</span>
            <span class="loader" style="display:none;"></span>
          </button>
        </div>
        <div id="searchResults" style="margin-top: 14px; display: flex; flex-direction: column; gap: 8px;"></div>
      </div>
    </div>

    <!-- Integrated Multi-Audio Video Player -->
    <div class="player-card" id="playerCard">
      <div class="player-header">
        <div class="player-title" id="playerTitle">Anime Stream Player</div>
        <div class="player-badge" id="playerStatus">Ready</div>
      </div>

      <video id="video" controls playsinline></video>

      <div class="player-controls-bar">
        <!-- Audio selector -->
        <div class="control-group">
          <span class="control-label">🔊 Audio Track:</span>
          <div id="audioBtnGroup" style="display:flex; gap:6px;"></div>
        </div>

        <!-- Video quality selector -->
        <div class="control-group">
          <span class="control-label">🎬 Quality:</span>
          <div id="qualityBtnGroup" style="display:flex; gap:6px;"></div>
        </div>
      </div>
    </div>

    <!-- Extracted Stream Links & Master Playlists -->
    <div class="card links-card" id="linksCard">
      <h3 style="font-size: 1.1rem; margin-bottom: 14px; color: #fff;">🎬 Synchronized Master Playlists & Direct Streams</h3>
      
      <div id="linksContainer"></div>

      <div style="margin-top: 20px;">
        <h4 style="font-size: 0.9rem; color: #9ca3af; margin-bottom: 8px;">💻 Ready-to-Run Terminal Commands:</h4>
        <pre id="commandsBox"></pre>
      </div>
    </div>
  </div>

  <div class="toast" id="toast">Copied to clipboard!</div>

  <script>
    let hls = null;
    let currentData = null;
    const video = document.getElementById('video');

    function switchNav(tabId, btn) {
      document.querySelectorAll('.nav-tab').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById(tabId).classList.add('active');
    }

    function setUrlPreset(url) {
      document.getElementById('directUrl').value = url;
      extractByUrl();
    }

    function setMalPreset(malId, ep) {
      document.getElementById('malId').value = malId;
      document.getElementById('epNum').value = ep;
      extractByMal();
    }

    function showToast(msg) {
      const toast = document.getElementById('toast');
      toast.textContent = msg;
      toast.style.display = 'block';
      setTimeout(() => { toast.style.display = 'none'; }, 2000);
    }

    function copyText(txt) {
      navigator.clipboard.writeText(txt);
      showToast("Link copied to clipboard!");
    }

    function setBtnLoading(btnId, isLoading) {
      const btn = document.getElementById(btnId);
      if (!btn) return;
      const lbl = btn.querySelector('.btn-lbl');
      const ldr = btn.querySelector('.loader');
      if (lbl) lbl.style.display = isLoading ? 'none' : 'inline';
      if (ldr) ldr.style.display = isLoading ? 'inline-block' : 'none';
      btn.disabled = isLoading;
    }

    async function extractByUrl() {
      const url = document.getElementById('directUrl').value.trim();
      if (!url) return alert("Please enter an AniZone URL or UUID");
      setBtnLoading('btnExtractUrl', true);
      try {
        const res = await fetch(`/api/extract?url=${encodeURIComponent(url)}`);
        const data = await res.json();
        if (data.detail) throw new Error(data.detail);
        renderExtraction(data);
      } catch (err) {
        alert("Extraction Failed: " + err.message);
      } finally {
        setBtnLoading('btnExtractUrl', false);
      }
    }

    async function extractByMal() {
      const malId = document.getElementById('malId').value.trim();
      const ep = document.getElementById('epNum').value.trim();
      if (!malId || !ep) return alert("Please enter MAL ID and Episode");
      setBtnLoading('btnExtractMal', true);
      try {
        const res = await fetch(`/api/mal/${malId}/${ep}`);
        const data = await res.json();
        if (data.detail) throw new Error(data.detail);
        renderExtraction(data);
      } catch (err) {
        alert("Extraction Failed: " + err.message);
      } finally {
        setBtnLoading('btnExtractMal', false);
      }
    }

    async function searchAnime() {
      const q = document.getElementById('searchQuery').value.trim();
      if (!q) return;
      setBtnLoading('btnSearch', true);
      const resContainer = document.getElementById('searchResults');
      resContainer.innerHTML = '<span style="color:var(--subtext); font-size:0.85rem;">Searching AniZone...</span>';
      try {
        const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
        const data = await res.json();
        resContainer.innerHTML = '';
        if (data.results && data.results.length > 0) {
          data.results.forEach(item => {
            const row = document.createElement('div');
            row.className = 'link-row';
            row.style.marginBottom = '6px';
            row.innerHTML = `
              <div class="link-info">
                <span class="link-name">🎌 ${item.title}</span>
                <span class="link-url">${item.url}</span>
              </div>
              <button class="btn-sm btn-play-sm" onclick="setUrlPreset('${item.url}/1')">▶ Play Ep 1</button>
            `;
            resContainer.appendChild(row);
          });
        } else {
          resContainer.innerHTML = '<span style="color:var(--subtext); font-size:0.85rem;">No anime found on AniZone for this query.</span>';
        }
      } catch (err) {
        resContainer.innerHTML = `<span style="color:#ef4444; font-size:0.85rem;">Search error: ${err.message}</span>`;
      } finally {
        setBtnLoading('btnSearch', false);
      }
    }

    function renderExtraction(data) {
      currentData = data;
      document.getElementById('playerTitle').textContent = data.title || "AniZone Stream";
      document.getElementById('playerCard').style.display = 'block';
      document.getElementById('linksCard').style.display = 'block';

      // Load English master playlist by default
      const masterUrl = data.masterPlaylists.englishDubDefault || data.masterPlaylists.originalCdnMaster;
      loadHlsStream(masterUrl);

      // Render links
      const container = document.getElementById('linksContainer');
      container.innerHTML = `
        <div class="link-row">
          <div class="link-info">
            <span class="link-name">🇬🇧 Synchronized Master (Default English Dub)</span>
            <span class="link-url">${data.masterPlaylists.englishDubDefault}</span>
          </div>
          <div class="link-actions">
            <button class="btn-sm btn-play-sm" onclick="loadHlsStream('${data.masterPlaylists.englishDubDefault}')">▶ Play</button>
            <button class="btn-sm" onclick="copyText('${data.masterPlaylists.englishDubDefault}')">Copy</button>
          </div>
        </div>

        <div class="link-row">
          <div class="link-info">
            <span class="link-name">🇯🇵 Synchronized Master (Default Japanese Sub)</span>
            <span class="link-url">${data.masterPlaylists.japaneseSubDefault}</span>
          </div>
          <div class="link-actions">
            <button class="btn-sm btn-play-sm" onclick="loadHlsStream('${data.masterPlaylists.japaneseSubDefault}')">▶ Play</button>
            <button class="btn-sm" onclick="copyText('${data.masterPlaylists.japaneseSubDefault}')">Copy</button>
          </div>
        </div>

        <div class="link-row">
          <div class="link-info">
            <span class="link-name">⚡ Original CDN Master M3U8</span>
            <span class="link-url">${data.masterPlaylists.originalCdnMaster}</span>
          </div>
          <div class="link-actions">
            <button class="btn-sm btn-play-sm" onclick="loadHlsStream('${data.masterPlaylists.originalCdnMaster}')">▶ Play</button>
            <button class="btn-sm" onclick="copyText('${data.masterPlaylists.originalCdnMaster}')">Copy</button>
          </div>
        </div>
      `;

      // Render video qualities
      data.videoQualities.forEach(v => {
        const row = document.createElement('div');
        row.className = 'link-row';
        row.innerHTML = `
          <div class="link-info">
            <span class="link-name">📹 Video Track: ${v.label}</span>
            <span class="link-url">${v.url}</span>
          </div>
          <div class="link-actions">
            <button class="btn-sm" onclick="copyText('${v.url}')">Copy</button>
          </div>
        `;
        container.appendChild(row);
      });

      // Render audio tracks
      data.audioTracks.forEach(a => {
        const flag = a.lang === 'en' ? '🇬🇧' : '🇯🇵';
        const row = document.createElement('div');
        row.className = 'link-row';
        row.innerHTML = `
          <div class="link-info">
            <span class="link-name">${flag} Audio Track: ${a.name} (${a.lang.toUpperCase()})</span>
            <span class="link-url">${a.url}</span>
          </div>
          <div class="link-actions">
            <button class="btn-sm" onclick="copyText('${a.url}')">Copy</button>
          </div>
        `;
        container.appendChild(row);
      });

      // Render commands
      document.getElementById('commandsBox').textContent = 
        `# 1. Play in MPV with English Dub:\n` + data.commands.mpv_english + `\n\n` +
        `# 2. Play in MPV with Japanese Audio:\n` + data.commands.mpv_japanese + `\n\n` +
        `# 3. Download to MP4 via FFmpeg:\n` + data.commands.ffmpeg_english_mp4;

      document.getElementById('playerCard').scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function loadHlsStream(streamUrl) {
      if (hls) {
        hls.destroy();
        hls = null;
      }

      const statusEl = document.getElementById('playerStatus');
      statusEl.textContent = 'Loading...';

      if (Hls.isSupported()) {
        hls = new Hls({ enableWorker: true });
        hls.loadSource(streamUrl);
        hls.attachMedia(video);

        hls.on(Hls.Events.MANIFEST_PARSED, () => {
          statusEl.textContent = 'Playing';
          buildAudioButtons();
          buildQualityButtons();
          video.play().catch(() => {});
        });

        hls.on(Hls.Events.AUDIO_TRACKS_UPDATED, () => {
          buildAudioButtons();
        });

        hls.on(Hls.Events.ERROR, (_, data) => {
          if (data.fatal) statusEl.textContent = 'Error: ' + data.type;
        });

      } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
        video.src = streamUrl;
        video.play().catch(() => {});
      }
    }

    function buildAudioButtons() {
      const group = document.getElementById('audioBtnGroup');
      group.innerHTML = '';
      if (!hls || !hls.audioTracks || hls.audioTracks.length === 0) return;

      hls.audioTracks.forEach((track, idx) => {
        const btn = document.createElement('button');
        const isEn = track.lang === 'en' || track.name.toLowerCase().includes('english');
        btn.className = 'btn-toggle' + (idx === hls.audioTrack ? ' active' : '');
        btn.textContent = (isEn ? '🇬🇧 ' : '🇯🇵 ') + track.name;
        btn.onclick = () => {
          hls.audioTrack = idx;
          document.querySelectorAll('#audioBtnGroup .btn-toggle').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          showToast(`Switched to ${track.name} audio`);
        };
        group.appendChild(btn);
      });
    }

    function buildQualityButtons() {
      const group = document.getElementById('qualityBtnGroup');
      group.innerHTML = '';
      if (!hls || !hls.levels || hls.levels.length === 0) return;

      // Auto button
      const autoBtn = document.createElement('button');
      autoBtn.className = 'btn-toggle' + (hls.currentLevel === -1 ? ' active' : '');
      autoBtn.textContent = 'Auto';
      autoBtn.onclick = () => {
        hls.currentLevel = -1;
        document.querySelectorAll('#qualityBtnGroup .btn-toggle').forEach(b => b.classList.remove('active'));
        autoBtn.classList.add('active');
      };
      group.appendChild(autoBtn);

      hls.levels.forEach((lvl, idx) => {
        const btn = document.createElement('button');
        btn.className = 'btn-toggle' + (idx === hls.currentLevel ? ' active' : '');
        btn.textContent = lvl.height ? `${lvl.height}p` : `${lvl.bitrate / 1000}k`;
        btn.onclick = () => {
          hls.currentLevel = idx;
          document.querySelectorAll('#qualityBtnGroup .btn-toggle').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
        };
        group.appendChild(btn);
      });
    }
  </script>
</body>
</html>
"""

PLAYER_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"/>
  <title>AniZone Iframe Stream Player</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }

    html, body {
      width: 100%;
      height: 100%;
      background: #000;
      color: #fff;
      font-family: 'Inter', system-ui, -apple-system, sans-serif;
      overflow: hidden;
    }

    .player-container {
      position: relative;
      width: 100%;
      height: 100%;
      background: #000;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    video {
      width: 100%;
      height: 100%;
      object-fit: contain;
      background: #000;
    }

    /* Floating Quick Bar for Audio & Quality */
    .overlay-bar {
      position: absolute;
      top: 14px;
      right: 14px;
      display: flex;
      gap: 8px;
      z-index: 50;
      background: rgba(15, 20, 30, 0.75);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border: 1px solid rgba(255, 255, 255, 0.12);
      padding: 6px 10px;
      border-radius: 12px;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.6);
      transition: opacity 0.3s ease, transform 0.3s ease;
      opacity: 0.85;
    }

    .overlay-bar:hover {
      opacity: 1;
      transform: translateY(-1px);
    }

    .btn-chip {
      background: rgba(255, 255, 255, 0.08);
      border: 1px solid rgba(255, 255, 255, 0.12);
      color: #e5e7eb;
      padding: 4px 10px;
      border-radius: 8px;
      font-size: 0.76rem;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 4px;
      transition: all 0.15s ease;
    }

    .btn-chip:hover {
      background: rgba(255, 255, 255, 0.2);
      color: #fff;
    }

    .btn-chip.active {
      background: linear-gradient(135deg, #8b5cf6 0%, #ec4899 100%);
      border-color: transparent;
      color: #fff;
      box-shadow: 0 2px 10px rgba(139, 92, 246, 0.4);
    }

    /* Loading Spinner */
    .loader-overlay {
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.7);
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 12px;
      z-index: 40;
      display: none;
    }

    .spinner {
      width: 42px;
      height: 42px;
      border: 3px solid rgba(255, 255, 255, 0.2);
      border-top-color: #8b5cf6;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }

    .loader-text {
      font-size: 0.85rem;
      font-weight: 600;
      color: #c4b5fd;
      letter-spacing: 0.02em;
    }

    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }

    /* Message Toast */
    .toast {
      position: absolute;
      bottom: 20px;
      left: 50%;
      transform: translateX(-50%);
      background: rgba(16, 185, 129, 0.9);
      color: #fff;
      padding: 6px 14px;
      border-radius: 8px;
      font-size: 0.78rem;
      font-weight: 600;
      z-index: 60;
      display: none;
      box-shadow: 0 4px 14px rgba(0,0,0,0.5);
    }
  </style>
</head>
<body>

  <div class="player-container">
    <video id="video" controls playsinline></video>

    <!-- Top floating track switcher overlay -->
    <div class="overlay-bar" id="overlayBar">
      <div id="audioGroup" style="display:flex; gap:4px; align-items:center;"></div>
      <div style="width:1px; height:18px; background:rgba(255,255,255,0.15); margin:0 2px;"></div>
      <div id="qualityGroup" style="display:flex; gap:4px; align-items:center;"></div>
    </div>

    <!-- Spinner Overlay -->
    <div class="loader-overlay" id="loaderOverlay">
      <div class="spinner"></div>
      <div class="loader-text" id="loaderText">Loading Stream...</div>
    </div>

    <!-- Toast Notification -->
    <div class="toast" id="toast">Switched track</div>
  </div>

  <script>
    const video = document.getElementById("video");
    const audioGroup = document.getElementById("audioGroup");
    const qualityGroup = document.getElementById("qualityGroup");
    const loaderOverlay = document.getElementById("loaderOverlay");
    const loaderText = document.getElementById("loaderText");
    const toast = document.getElementById("toast");

    let hls = null;
    let preferredAudio = "en";

    function showToast(msg) {
      toast.textContent = msg;
      toast.style.display = "block";
      setTimeout(() => { toast.style.display = "none"; }, 2000);
    }

    function setLoader(show, text = "Loading Stream...") {
      loaderText.textContent = text;
      loaderOverlay.style.display = show ? "flex" : "none";
    }

    function loadStream(url) {
      if (!url) return;
      if (hls) {
        hls.destroy();
        hls = null;
      }

      setLoader(true, "Loading Anime Stream...");

      if (Hls.isSupported()) {
        hls = new Hls({
          enableWorker: true,
          lowLatencyMode: true,
          backBufferLength: 90
        });

        hls.loadSource(url);
        hls.attachMedia(video);

        hls.on(Hls.Events.MANIFEST_PARSED, () => {
          setLoader(false);
          renderAudioButtons();
          renderQualityButtons();
          selectDefaultAudio();
          video.play().catch(() => {});
        });

        hls.on(Hls.Events.AUDIO_TRACKS_UPDATED, () => {
          renderAudioButtons();
        });

        hls.on(Hls.Events.ERROR, (_, data) => {
          if (data.fatal) {
            setLoader(true, "Stream error: " + data.type);
            console.error("HLS Error:", data);
          }
        });

      } else if (video.canPlayType("application/vnd.apple.mpegurl")) {
        video.src = url;
        video.addEventListener("loadedmetadata", () => {
          setLoader(false);
          video.play().catch(() => {});
        });
      }
    }

    function selectDefaultAudio() {
      if (!hls || !hls.audioTracks || hls.audioTracks.length === 0) return;
      const targetIdx = hls.audioTracks.findIndex(t => {
        const lang = (t.lang || "").toLowerCase();
        const name = (t.name || "").toLowerCase();
        if (preferredAudio === "en") return lang === "en" || lang === "eng" || name.includes("english") || name.includes("dub");
        if (preferredAudio === "ja") return lang === "ja" || lang === "jpn" || name.includes("japanese") || name.includes("sub");
        return false;
      });

      if (targetIdx !== -1 && hls.audioTrack !== targetIdx) {
        hls.audioTrack = targetIdx;
      }
      renderAudioButtons();
    }

    function renderAudioButtons() {
      audioGroup.innerHTML = "";
      if (!hls || !hls.audioTracks || hls.audioTracks.length <= 1) {
        audioGroup.style.display = "none";
        return;
      }
      audioGroup.style.display = "flex";

      hls.audioTracks.forEach((track, idx) => {
        const btn = document.createElement("button");
        const isEn = (track.lang || "").toLowerCase().includes("en") || (track.name || "").toLowerCase().includes("english");
        btn.className = "btn-chip" + (idx === hls.audioTrack ? " active" : "");
        btn.innerHTML = (isEn ? "🇬🇧 " : "🇯🇵 ") + track.name;
        btn.onclick = () => {
          hls.audioTrack = idx;
          preferredAudio = isEn ? "en" : "ja";
          document.querySelectorAll("#audioGroup .btn-chip").forEach(b => b.classList.remove("active"));
          btn.classList.add("active");
          showToast(`Audio: ${track.name}`);
        };
        audioGroup.appendChild(btn);
      });
    }

    function renderQualityButtons() {
      qualityGroup.innerHTML = "";
      if (!hls || !hls.levels || hls.levels.length <= 1) {
        qualityGroup.style.display = "none";
        return;
      }
      qualityGroup.style.display = "flex";

      const autoBtn = document.createElement("button");
      autoBtn.className = "btn-chip" + (hls.currentLevel === -1 ? " active" : "");
      autoBtn.textContent = "Auto";
      autoBtn.onclick = () => {
        hls.currentLevel = -1;
        document.querySelectorAll("#qualityGroup .btn-chip").forEach(b => b.classList.remove("active"));
        autoBtn.classList.add("active");
        showToast("Quality: Auto");
      };
      qualityGroup.appendChild(autoBtn);

      hls.levels.forEach((lvl, idx) => {
        const btn = document.createElement("button");
        btn.className = "btn-chip" + (idx === hls.currentLevel ? " active" : "");
        btn.textContent = lvl.height ? `${lvl.height}p` : `${Math.round(lvl.bitrate / 1000)}k`;
        btn.onclick = () => {
          hls.currentLevel = idx;
          document.querySelectorAll("#qualityGroup .btn-chip").forEach(b => b.classList.remove("active"));
          btn.classList.add("active");
          showToast(`Quality: ${lvl.height ? lvl.height + 'p' : 'Custom'}`);
        };
        qualityGroup.appendChild(btn);
      });
    }

    window.addEventListener("message", (event) => {
      const data = event.data;
      if (!data) return;

      if (data.type === "PLAY_STREAM" && data.url) {
        if (data.audio) preferredAudio = data.audio;
        loadStream(data.url);
      } else if (data.type === "SET_AUDIO" && data.audio) {
        preferredAudio = data.audio;
        selectDefaultAudio();
      } else if (data.type === "SET_QUALITY" && typeof data.level === "number") {
        if (hls) hls.currentLevel = data.level;
        renderQualityButtons();
      }
    });

    const urlParams = new URLSearchParams(window.location.search);
    const streamUrl = urlParams.get("url");
    const uuidParam = urlParams.get("uuid");
    const audioParam = urlParams.get("audio");

    if (audioParam) preferredAudio = audioParam;

    if (streamUrl) {
      loadStream(streamUrl);
    } else if (uuidParam) {
      loadStream(`/playlist/${uuidParam}/master.m3u8?audio=${preferredAudio}`);
    } else {
      loadStream(`/playlist/8d3f2c88-e5a6-4806-8f89-14f8e09b5238/master.m3u8?audio=en`);
    }
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_UI


@app.get("/player", response_class=HTMLResponse)
async def player_page():
    return PLAYER_HTML


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"[*] Starting AniZone Web Service on http://0.0.0.0:{port}...")
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)

