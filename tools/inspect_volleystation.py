"""One-shot VolleyStation public widget network inspection.

This diagnostics script uses a real Chromium browser to discover requests
performed by the publicly embedded VolleyStation widgets. It does not bypass
authentication, read browser secrets, or scrape protected data.

Run with:
  pip install playwright
  python -m playwright install chromium
  python -m tools.inspect_volleystation
"""
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright

MATCH_ID = "2543574"
TARGETS = [
    f"https://widgets.volleystation.com/play-by-play/{MATCH_ID}",
    f"https://widgets.volleystation.com/team-stats/{MATCH_ID}",
]
OUTPUT = Path("captures/volleystation")
ALLOWED_HOSTS = ("volleystation.com", "volleystation.pl")
MAX_RESPONSE_SIZE = 80_000

def relevant(url):
    host = urlparse(url).hostname or ""
    return any(host == suffix or host.endswith("." + suffix) for suffix in ALLOWED_HOSTS)

async def inspect(page, url):
    records = []
    tasks = []
    async def record(response):
        if not relevant(response.url):
            return
        req = response.request
        entry = {
            "url": response.url,
            "method": req.method,
            "type": req.resource_type,
            "status": response.status,
            "content_type": response.headers.get("content-type", ""),
        }
        records.append(entry)
        ctype = entry["content_type"].lower()
        if ("json" in ctype or "text" in ctype or "javascript" in ctype) and req.resource_type in ("fetch", "xhr"):
            try:
                body = await response.body()
                # Store only a small response preview; avoid large transfers
                entry["body_preview"] = body[:MAX_RESPONSE_SIZE].decode("utf-8", errors="replace")
            except Exception as exc:
                entry["response_error"] = type(exc).__name__

    def on_response(response):
        tasks.append(asyncio.create_task(record(response)))
    page.on("response", on_response)
    async def on_ws(ws):
        if not relevant(ws.url):
            return
        entry = {"websocket": ws.url, "messages": []}
        records.append(entry)
        ws.on("framereceived", lambda payload: entry["messages"].append(str(payload)[:400]) if len(entry["messages"]) < 5 else None)

    page.on("websocket", lambda ws: asyncio.create_task(on_ws(ws)))
    result = {"page": url, "requests": records}
    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        result["document_status"] = response.status if response else None
        await page.wait_for_timeout(5000)
        result["title"] = await page.title()
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return result

async def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    report = {"captured_at_utc": datetime.now(timezone.utc).isoformat(),
              "match_id": MATCH_ID, "widgets": []}
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            for url in TARGETS:
                page = await browser.new_page()
                report["widgets"].append(await inspect(page, url))
                await page.close()
        finally:
            await browser.close()
    (OUTPUT / "inspection.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    for item in report["widgets"]:
        print("PAGE", item["page"], "STATUS", item.get("document_status"), "ERROR", item.get("error"))
        for record in item["requests"]:
            print(record.get("method", "WS"), record.get("status", ""), record.get("type", ""), record.get("url", record.get("websocket")))

if __name__ == "__main__":
    asyncio.run(main())
