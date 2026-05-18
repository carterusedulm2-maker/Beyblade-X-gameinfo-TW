#!/usr/bin/env python3
"""
戰鬥陀螺比賽資訊爬蟲 v4
========================================
策略：
1. 預先載入 KNOWN_GOOD_LINKS（不管 HackMD 有沒有）
2. 從 HackMD 解析所有期程的 Spreadsheet URL
3. 對每個 URL 嘗試抓取 CSV
4. 如果某期程 HackMD URL 抓不到資料 → 用現有 events.json 裡的舊資料保留
5. 如果是最新期程且 HackMD 失敗 → 用已知有效的 direct link 作為 fallback
"""

import csv
import io
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, date, timedelta, timezone
from collections import Counter


HACKMD_URL = "https://hackmd.io/@liangyutw/beyblade-important-record"
SHEETS_CSV_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"
GOOGLE_SEARCH_HTML_URL = "https://www.google.com/search"
BING_SEARCH_HTML_URL = "https://www.bing.com/search"
TAIPEI_TZ = timezone(timedelta(hours=8))

# 已知有效的直接連結（當 HackMD URL 抓不到或抓到少於預期時使用）
# 格式：period -> {source_name: sheet_id}
KNOWN_GOOD_LINKS = {
    "2026-5、6月": {
        "B4合作據點G3 (2026-5、6月)": "10fcGvFS9W_-pVQSb0Pv3QJB2jVktM3kw34dY0ymuiqY",
    },
}

CITIES = [
    "台北市", "新北市", "桃園市", "台中市", "台南市", "高雄市",
    "基隆市", "新竹市", "新竹縣", "苗栗縣", "彰化縣", "南投縣",
    "雲林縣", "嘉義市", "嘉義縣", "屏東縣", "宜蘭縣", "花蓮縣",
    "台東縣", "澎湖縣", "金門縣", "連江縣",
]
CITY_NORMALIZE = {
    "臺北市": "台北市", "臺南市": "台南市", "臺中市": "台中市",
    "花蓮市": "花蓮縣", "屏東市": "屏東縣", "彰化市": "彰化縣",
    "宜蘭市": "宜蘭縣", "竹北市": "新竹縣",
}
DISTRICT_TO_CITY = {
    "桃園區": "桃園市", "中壢區": "桃園市", "板橋區": "新北市",
    "三重區": "新北市", "新莊區": "新北市", "中和區": "新北市",
    "永和區": "新北市", "新店區": "新北市", "樹林區": "新北市",
}
MAX_RETRIES = 3


def fetch_url(url: str) -> str:
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}")
                return resp.read().decode("utf-8-sig")
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                print(f"    Retry {attempt + 1}/{MAX_RETRIES}: {e}")
    raise last_err


def today_taipei() -> date:
    return datetime.now(TAIPEI_TZ).date()


def extract_city(address: str) -> str:
    addr = re.sub(r'^\d{3,6}', '', address)
    for city in CITIES:
        if city in addr:
            return CITY_NORMALIZE.get(city, city)
    if len(addr) >= 3 and addr[2] in ("市", "縣"):
        raw = addr[:3]
        return CITY_NORMALIZE.get(raw, raw)
    if addr.startswith("北市"):
        return "台北市"
    for dist, city in DISTRICT_TO_CITY.items():
        if addr.startswith(dist):
            return city
    m = re.search(r'\d{3}([^\d]{2}[市縣])', address)
    if m:
        return CITY_NORMALIZE.get(m.group(1), m.group(1))
    return ""


def determine_year(month: int) -> int:
    now = today_taipei()
    if month < now.month - 6:
        return now.year + 1
    return now.year


def parse_date(date_str: str) -> str:
    date_str = date_str.strip()
    if not date_str:
        return ""
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    parts = date_str.replace("月", "/").replace("日", "").split("/")
    try:
        if len(parts) == 3:
            a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
            if a > 100:
                return f"{a}-{b:02d}-{c:02d}"
            elif c > 100:
                return f"{c}-{a:02d}-{b:02d}"
        elif len(parts) == 2:
            month, day = int(parts[0]), int(parts[1])
            year = determine_year(month)
            return f"{year}-{month:02d}-{day:02d}"
    except (ValueError, IndexError):
        pass
    return ""


def parse_time(time_str: str) -> str:
    time_str = time_str.strip()
    if not time_str:
        return ""
    time_str = time_str.replace("：", ":").strip()
    if re.match(r'^\d{1,2}:\d{2}$', time_str):
        return time_str
    m = re.search(r'(\d{1,2}:\d{2})', time_str)
    if m:
        return m.group(1)
    return ""


def parse_capacity(cap_str: str) -> int:
    cap_str = str(cap_str).strip()
    digits = re.sub(r'[^\d]', '', cap_str)
    if digits:
        try:
            return int(digits)
        except ValueError:
            pass
    return 0


def parse_csv_sheet(csv_text: str, source_name: str) -> list:
    events = []
    reader = csv.reader(io.StringIO(csv_text))
    header_found = False
    skipped = 0
    for row in reader:
        if not row or all(c.strip() == "" for c in row):
            continue
        if not header_found:
            row_str = "".join(row)
            if "店家名稱" in row_str:
                header_found = True
            continue
        if len(row) < 9:
            skipped += 1
            continue
        seq, store, phone, address, event_date, event_time, capacity, reg_method, category = row[:9]
        store = store.strip()
        if not store:
            continue
        if any(kw in store for kw in ["注意", "本公司", "活動", "贈品"]):
            continue
        parsed_date = parse_date(event_date)
        if not parsed_date:
            skipped += 1
            continue
        city = extract_city(address.strip())
        notes = row[9].strip() if len(row) > 9 and row[9].strip() else ""
        events.append({
            "source": source_name,
            "storeName": store,
            "phone": phone.strip(),
            "address": address.strip(),
            "city": city,
            "date": parsed_date,
            "time": parse_time(event_time),
            "capacity": parse_capacity(capacity),
            "registrationMethod": reg_method.strip(),
            "category": category.strip(),
            "notes": notes,
        })
    if skipped > 0:
        print(f"    Skipped {skipped} malformed rows")
    return events


def detect_source_type(csv_text: str) -> str:
    preview = "\n".join(csv_text.splitlines()[:8])
    if any(kw in preview for kw in ["B4通路", "B4合作", "合作據點"]):
        return "B4合作據點G3"
    if any(kw in preview for kw in ["門市G3", "Funbox", "FUNBOX", "百貨"]):
        return "Funbox門市G3"
    return ""


def fetch_sheet_csv(sheet_id: str) -> tuple:
    url = SHEETS_CSV_URL.format(sheet_id=sheet_id)
    try:
        csv_text = fetch_url(url)
        return csv_text, None
    except Exception as e:
        return "", str(e)


def fetch_sheet(source_name: str, sheet_id: str) -> tuple:
    csv_text, err = fetch_sheet_csv(sheet_id)
    if err:
        return [], err
    return parse_csv_sheet(csv_text, source_name), None


def normalize_period(period: str) -> str:
    return re.sub(r'(\d{4})[-年](\d{1,2})[、/](\d{1,2})月',
                  r'\1-\2、\3月', period.strip())


def parse_period(period: str) -> tuple:
    m = re.match(r'(\d{4})-(\d{1,2})、(\d{1,2})月', normalize_period(period))
    if not m:
        return 0, 0, 0
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def make_period(year: int, month: int) -> str:
    start_month = month if month % 2 == 1 else month - 1
    return f"{year}-{start_month}、{start_month + 1}月"


def add_months(year: int, month: int, months: int) -> tuple:
    month_index = (year * 12) + (month - 1) + months
    return month_index // 12, (month_index % 12) + 1


def target_search_periods() -> list:
    today = today_taipei()
    current = make_period(today.year, today.month)
    next_year, next_month = add_months(today.year, today.month, 2)
    next_period = make_period(next_year, next_month)
    return list(dict.fromkeys([current, next_period]))


def period_search_terms(period: str) -> list:
    year, start_month, end_month = parse_period(period)
    if not year:
        return [period]
    return [
        period,
        f"{year} {start_month} {end_month}月",
        f"{year} {start_month}~{end_month}月",
        f"{year} {start_month}~{end_month}",
    ]


def extract_sheet_id(cell: str) -> str:
    m = re.search(r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)', cell)
    return m.group(1) if m else ""


def extract_sheet_ids(text: str) -> list:
    decoded = urllib.parse.unquote(text)
    ids = re.findall(r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)', decoded)
    return list(dict.fromkeys(ids))


def parse_hackmd_all_sheets(html: str) -> list:
    results = []
    seen_periods = set()
    period_pattern = re.compile(r'^\s*\|\s*(\d{4}[-年]\d{1,2}[、/]\d{1,2}月)\s*\|')

    for line in html.splitlines():
        m = period_pattern.match(line)
        if not m:
            continue

        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue

        period_norm = normalize_period(cells[0])
        if period_norm in seen_periods:
            continue

        funbox_id = extract_sheet_id(cells[1])
        b4_id = extract_sheet_id(cells[2])
        if not funbox_id and not b4_id:
            continue

        seen_periods.add(period_norm)
        results.append({
            "period": period_norm,
            "funbox_id": funbox_id,
            "b4_id": b4_id,
        })
        funbox_label = f"{funbox_id[:16]}..." if funbox_id else "missing"
        b4_label = f"{b4_id[:16]}..." if b4_id else "missing"
        print(f"    {period_norm}: Funbox={funbox_label}, B4={b4_label}")

    print(f"  Found {len(results)} period rows in HackMD")
    return results


def search_google_cse(query: str) -> str:
    api_key = os.environ.get("GOOGLE_SEARCH_API_KEY", "")
    cx = os.environ.get("GOOGLE_SEARCH_CX", "")
    if not api_key or not cx:
        return ""
    params = urllib.parse.urlencode({
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": 10,
    })
    data = json.loads(fetch_url(f"{GOOGLE_CSE_URL}?{params}"))
    chunks = []
    for item in data.get("items", []):
        chunks.append(item.get("link", ""))
        chunks.append(item.get("snippet", ""))
    return "\n".join(chunks)


def search_public_html(query: str) -> str:
    providers = [
        ("Google", GOOGLE_SEARCH_HTML_URL, {"q": query, "num": "10", "hl": "zh-TW"}),
        ("Bing", BING_SEARCH_HTML_URL, {"q": query, "count": "10"}),
    ]
    chunks = []
    for name, base_url, params in providers:
        try:
            url = f"{base_url}?{urllib.parse.urlencode(params)}"
            chunks.append(fetch_url(url))
        except Exception as e:
            print(f"    {name} search failed: {e}")
    return "\n".join(chunks)


def search_sheet_ids(query: str) -> list:
    texts = []
    try:
        cse_text = search_google_cse(query)
        if cse_text:
            texts.append(cse_text)
    except Exception as e:
        print(f"    Google CSE search failed: {e}")

    if not texts:
        try:
            texts.append(search_public_html(query))
            time.sleep(1)
        except Exception as e:
            print(f"    Public search failed: {e}")

    ids = []
    for text in texts:
        ids.extend(extract_sheet_ids(text))
    return list(dict.fromkeys(ids))


def discover_sheets_by_search(existing_sheet_ids: set, loaded_source_names: set) -> list:
    discovered = []
    seen = set(existing_sheet_ids)
    for period in target_search_periods():
        missing_types = [
            source_type for source_type in ("Funbox門市G3", "B4合作據點G3")
            if get_source_name(source_type, period) not in loaded_source_names
        ]
        if not missing_types:
            print(f"  {period}: already has Funbox and B4 sources, skipping search")
            continue
        for term in period_search_terms(period):
            queries = [f'"戰鬥陀螺X" "比賽資訊" "{term}" "docs.google.com/spreadsheets"']
            if "Funbox門市G3" in missing_types:
                queries.append(f'"戰鬥陀螺X" "G3" "{term}" "docs.google.com/spreadsheets"')
            if "B4合作據點G3" in missing_types:
                queries.append(f'"戰鬥陀螺X" "B4" "{term}" "docs.google.com/spreadsheets"')
            for query in queries:
                print(f"  Searching: {query}")
                for sheet_id in search_sheet_ids(query):
                    if sheet_id in seen:
                        continue
                    seen.add(sheet_id)
                    csv_text, err = fetch_sheet_csv(sheet_id)
                    if err:
                        print(f"    ✗ {sheet_id[:16]}... fetch failed: {err}")
                        continue
                    source_type = detect_source_type(csv_text)
                    if not source_type:
                        print(f"    - {sheet_id[:16]}... skipped: unknown sheet type")
                        continue
                    if source_type not in missing_types:
                        print(f"    - {sheet_id[:16]}... skipped: {source_type} already loaded")
                        continue
                    events = parse_csv_sheet(csv_text, get_source_name(source_type, period))
                    if not events:
                        print(f"    - {sheet_id[:16]}... skipped: no parseable events")
                        continue
                    discovered.append({
                        "period": period,
                        "source_type": source_type,
                        "sheet_id": sheet_id,
                        "events": events,
                        "query": query,
                    })
                    print(f"    ✓ {source_type} ({period}): {len(events)} events from search")
    return discovered


def get_source_name(source_type: str, period: str) -> str:
    return f"{source_type} ({period})"


def main():
    # Load existing events.json as fallback
    existing_events = []
    try:
        with open("events.json", "r") as f:
            existing_data = json.load(f)
            existing_events = existing_data.get("events", [])
            print(f"Loaded {len(existing_events)} events from existing events.json")
            existing_by_source = {}
            for e in existing_events:
                src = e["source"]
                if src not in existing_by_source:
                    existing_by_source[src] = []
                existing_by_source[src].append(e)
    except Exception as e:
        print(f"  No existing events.json found: {e}")
        existing_by_source = {}

    all_events = []
    all_sources = []
    sources_processed = set()  # Track which (period, type) we've handled

    # =================================================================
    # STEP 1: PRE-LOAD from KNOWN_GOOD_LINKS
    # This runs FIRST so that even if HackMD doesn't have the latest
    # period (e.g. 5-6月), we still include the data.
    # =================================================================
    print("\n[Step 1] Pre-loading KNOWN_GOOD_LINKS...")
    for period, sources_dict in KNOWN_GOOD_LINKS.items():
        for src_name, sheet_id in sources_dict.items():
            evs, err = fetch_sheet(src_name, sheet_id)
            if evs:
                all_events.extend(evs)
                all_sources.append({"name": src_name, "sheet_id": sheet_id, "source": "known_good"})
                sources_processed.add(src_name)
                print(f"  ★ {src_name}: {len(evs)} events (from KNOWN_GOOD_LINKS)")
            else:
                # Fall back to existing events for this source
                existing = [e for e in existing_events if e["source"] == src_name]
                if existing:
                    all_events.extend(existing)
                    all_sources.append({"name": src_name, "sheet_id": sheet_id, "source": "existing"})
                    print(f"  ~ {src_name}: kept {len(existing)} existing events (known_good failed: {err})")
                else:
                    print(f"  ✗ {src_name}: no events (known_good failed: {err}), no existing fallback")

    # =================================================================
    # STEP 2: Fetch and process HackMD
    # =================================================================
    print("\n[Step 2] Fetching HackMD index page...")
    try:
        hackmd_html = fetch_url(HACKMD_URL)
        hackmd_rows = parse_hackmd_all_sheets(hackmd_html)
    except Exception as e:
        print(f"HackMD fetch failed: {e}")
        hackmd_rows = []

    for row in hackmd_rows:
        period = row["period"]

        # ---- Funbox ----
        funbox_name = get_source_name("Funbox門市G3", period)
        funbox_events = []
        if funbox_name in sources_processed:
            existing = [e for e in all_events if e["source"] == funbox_name]
            print(f"  → {funbox_name}: already pre-loaded ({len(existing)} events), skipping HackMD")
            funbox_events = existing
        elif row["funbox_id"]:
            funbox_events, funbox_err = fetch_sheet(funbox_name, row["funbox_id"])
            if funbox_events:
                all_events.extend(funbox_events)
                all_sources.append({"name": funbox_name, "sheet_id": row["funbox_id"]})
                sources_processed.add(funbox_name)
                print(f"  ✓ {funbox_name}: {len(funbox_events)} events")
            else:
                existing = [e for e in existing_events if e["source"] == funbox_name]
                if existing:
                    all_events.extend(existing)
                    all_sources.append({"name": funbox_name, "sheet_id": row["funbox_id"], "source": "existing"})
                    print(f"  ~ {funbox_name}: kept {len(existing)} existing events (HackMD failed: {funbox_err})")
                else:
                    print(f"  ✗ {funbox_name}: no events, no fallback (error: {funbox_err})")
        else:
            existing = [e for e in existing_events if e["source"] == funbox_name]
            if existing:
                all_events.extend(existing)
                all_sources.append({"name": funbox_name, "sheet_id": "", "source": "existing"})
                funbox_events = existing
                print(f"  ~ {funbox_name}: kept {len(existing)} existing events (HackMD URL missing)")
            else:
                print(f"  - {funbox_name}: HackMD URL missing, no existing fallback")

        # ---- B4 ----
        b4_name = get_source_name("B4合作據點G3", period)
        if b4_name in sources_processed:
            existing = [e for e in all_events if e["source"] == b4_name]
            print(f"  → {b4_name}: already pre-loaded ({len(existing)} events), skipping HackMD")
        else:
            if row["b4_id"]:
                b4_events, b4_err = fetch_sheet(b4_name, row["b4_id"])
            else:
                b4_events, b4_err = [], "HackMD URL missing"
            b4_is_wrong = (len(b4_events) > 0 and funbox_events and
                           len(b4_events) == len(funbox_events))

            if b4_events and not b4_is_wrong:
                all_events.extend(b4_events)
                all_sources.append({"name": b4_name, "sheet_id": row["b4_id"]})
                sources_processed.add(b4_name)
                print(f"  ✓ {b4_name}: {len(b4_events)} events")
            else:
                direct_link_used = False
                if b4_is_wrong:
                    print(f"  B4 URL returns same as Funbox ({len(b4_events)}) -- likely HackMD error")
                if period in KNOWN_GOOD_LINKS and b4_name in KNOWN_GOOD_LINKS[period]:
                    direct_id = KNOWN_GOOD_LINKS[period][b4_name]
                    print(f"  Trying known good direct link for {period}...")
                    b4_events2, b4_err2 = fetch_sheet(b4_name, direct_id)
                    if b4_events2:
                        all_events.extend(b4_events2)
                        all_sources.append({"name": b4_name, "sheet_id": direct_id, "source": "known_good_direct"})
                        sources_processed.add(b4_name)
                        print(f"  ✓ {b4_name} (direct): {len(b4_events2)} events")
                        direct_link_used = True

                if not direct_link_used:
                    existing = [e for e in existing_events if e["source"] == b4_name]
                    if existing:
                        all_events.extend(existing)
                        all_sources.append({"name": b4_name, "sheet_id": row["b4_id"], "source": "existing"})
                        print(f"  ~ {b4_name}: kept {len(existing)} existing events")
                    else:
                        err_msg = b4_err if b4_is_wrong else f"no events (error: {b4_err})"
                        print(f"  ✗ {b4_name}: {err_msg}")

    # =================================================================
    # STEP 3: Best-effort search discovery for current/next period sheets
    # =================================================================
    print("\n[Step 3] Searching for current/next period Google Sheets...")
    known_sheet_ids = {s.get("sheet_id") for s in all_sources if s.get("sheet_id")}
    for row in hackmd_rows:
        if row.get("funbox_id"):
            known_sheet_ids.add(row["funbox_id"])
        if row.get("b4_id"):
            known_sheet_ids.add(row["b4_id"])

    for found in discover_sheets_by_search(known_sheet_ids, sources_processed):
        source_name = get_source_name(found["source_type"], found["period"])
        if source_name in sources_processed:
            print(f"  → {source_name}: already loaded, skipping searched sheet")
            continue
        all_events.extend(found["events"])
        all_sources.append({
            "name": source_name,
            "sheet_id": found["sheet_id"],
            "source": "web_search",
            "query": found["query"],
        })
        sources_processed.add(source_name)

    # Validate
    if not all_events:
        print("\nERROR: No events found at all!")
        sys.exit(1)

    # Deduplicate
    seen = set()
    deduped = []
    for ev in all_events:
        key = (ev["date"], ev["storeName"], ev["time"])
        if key not in seen:
            seen.add(key)
            deduped.append(ev)

    today_str = today_taipei().isoformat()
    before_cleanup = len(deduped)
    deduped = [e for e in deduped if e["date"] >= today_str]
    removed = before_cleanup - len(deduped)
    print(f"\nRemoved {removed} expired events before {today_str}")

    if not deduped:
        print("\nERROR: No upcoming events found after removing expired events!")
        sys.exit(1)

    active_sources = {e["source"] for e in deduped}
    all_sources = [s for s in all_sources if s["name"] in active_sources]

    # Sort and assign IDs
    deduped.sort(key=lambda e: e["date"], reverse=True)
    for i, event in enumerate(deduped, 1):
        prefix = "funbox" if "Funbox" in event["source"] else "b4"
        event["id"] = f"{prefix}-{i:03d}"

    output = {
        "lastUpdated": today_str,
        "sources": all_sources,
        "events": deduped,
    }

    with open("events.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Done! {len(deduped)} events written to events.json")

    months = Counter(e['date'][:7] for e in deduped)
    print("\n每月比賽數：")
    for m, n in sorted(months.items()):
        print(f"  {m}: {n} 場")


if __name__ == "__main__":
    main()
