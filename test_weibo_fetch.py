#!/usr/bin/env python3
"""
Weibo fetch test — standalone script.

Usage:
    uv run python test_weibo_fetch.py <uid>

Example:
    uv run python test_weibo_fetch.py 5194153520
"""

import asyncio
import json
import logging
import sys

import aiohttp

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("weibo_test")

# Built-in pool of real Chrome user-agent strings (same as live_monitor.monitor.weibo_dynamic)
_CHROME_UA_POOL = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
]

import random
def _get_random_ua() -> str:
    return random.choice(_CHROME_UA_POOL)


def make_headers(uid: str) -> dict[str, str]:
    ua_str = _get_random_ua()
    return {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "mweibo-pwa": "1",
        "referer": f"https://m.weibo.cn/u/{uid}",
        "sec-ch-ua": '"Chromium";v="125", "Not.A/Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": ua_str,
        "User-Agent": ua_str,
        "x-requested-with": "XMLHttpRequest",
    }


async def fetch_weibo(uid: int, cookie: str = ""):
    uid_str = str(uid)
    url = (
        f"https://m.weibo.cn/api/container/getIndex"
        f"?type=uid&value={uid_str}&containerid=107603{uid_str}&count=5"
    )
    headers = make_headers(uid_str)
    if cookie:
        headers["cookie"] = cookie

    print(f"\n{'='*60}")
    print(f"🔍 Fetching Weibo UID: {uid}")
    print(f"URL: {url}")
    print(f"{'='*60}\n")

    async with aiohttp.ClientSession() as session:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            print(f"HTTP Status: {resp.status}")
            print(f"Headers: {dict(resp.headers)}\n")

            if resp.status != 200:
                body = await resp.text()
                print(f"Response body (first 500 chars):\n{body[:500]}")
                return

            body = await resp.text()
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error: {e}")
                print(f"Raw body (first 500 chars):\n{body[:500]}")
                return

            ok = data.get("ok", 0)
            msg = data.get("msg", "")
            print(f"API ok={ok}, msg={msg!r}")

            if ok != 1:
                print(f"❌ API returned ok={ok}")
                return

            result = data.get("data", {})
            cards = result.get("cards", [])

            print(f"Number of cards: {len(cards)}")

            if not cards:
                print(f"data keys: {list(result.keys())}")
                print("❌ No cards returned")
                return

            for i, card in enumerate(cards[:5]):
                mblog = card.get("mblog")
                card_type = card.get("card_type")
                if mblog:
                    user = mblog.get("user", {})
                    screen_name = user.get("screen_name", "?")
                    mblog_id = mblog.get("id", "?")
                    created_at = mblog.get("created_at", "?")
                    text = mblog.get("text", "")[:100]
                    is_top = mblog.get("isTop", 0)

                    print(f"\n--- Card #{i+1} (type={card_type}, top={is_top}) ---")
                    print(f"  User: {screen_name}")
                    print(f"  ID: {mblog_id}")
                    print(f"  Created: {created_at}")
                    print(f"  Text: {text[:80]}...")
                else:
                    print(f"\n--- Card #{i+1} (type={card_type}, NO mblog) ---")
                    print(f"  Keys: {list(card.keys())[:10]}")

            print(f"\n{'='*60}")
            print("✅ Done")
            print(f"{'='*60}")


def main():
    if len(sys.argv) < 2:
        print("Usage: uv run python test_weibo_fetch.py <uid>")
        print("Example: uv run python test_weibo_fetch.py 5194153520")
        sys.exit(1)

    uid = int(sys.argv[1])
    cookie = sys.argv[2] if len(sys.argv) > 2 else ""

    asyncio.run(fetch_weibo(uid, cookie))


if __name__ == "__main__":
    main()
