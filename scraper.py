#!/usr/bin/env python3
"""
戰鬥陀螺比賽資訊爬蟲 v3
========================================
策略：
1. 從 HackMD 解析所有期程的 Spreadsheet URL
2. 對每個 URL 嘗試抓取 CSV
3. 如果某期程 HackMD URL 抓不到資料 → 用現有 events.json 裡的舊資料保留
4. 如果是最新期程且 HackMD 失敗 → 用已知有效的 direct link 作為 fallback
"""

import csv
import io
import json
import re
import sys
import urllib.request
from datetime import datetime, date
from collections import Counter


HACKMD_URL = "https://hackmd.io/@liangyutw/beyblade-important-record"
SHEETS_CSV_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

# 最新期程（5-6月）的已知有效 direct link（HackMD 可能未即時更新）
# 當 HackMD 對最新期程抓不到資料時使用
# 已知有效的直接連結（當 HackMD URL 抓不到或抓到少於預期時使用）
# 格式：period -> {source_name: sheet_id}
KNOWN_GOOD_LINKS = {
    # Carter 分享的 B4 5-6月連結（HackMD 的版本是錯的）
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
    now = datetime.now()
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


def fetch_sheet(source_name: str, sheet_id: str) -> tuple:
    """Try to fetch a sheet. Returns (events, error_msg)."""
    url = SHEETS_CSV_URL.format(sheet_id=sheet_id)
    try:
        csv_text = fetch_url(url)
        events = parse_csv_sheet(csv_text, source_name)
        return events, None
    except Exception as e:
        return [], str(e)


def parse_hackmd_all_sheets(html: str) -> list:
    """Parse all period rows from HackMD markdown."""
    results = []
    # Pattern: | period | [G3](url) | [B4](url) |
    # Handle both /edit?gid=... and /edit?usp=sharing variants
    row_pattern = re.compile(
        r'\|\s*(\d{4}[-年]\d{1,2}[、/]\d{1,2}月)\s*\|'
        r'\s*\[G3\]\(https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)(?:/[^)]+)?\)\s*\|'
        r'\s*\[B4\]\(https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)(?:/[^)]+)?\)\s*\|'
    )
    matches = list(row_pattern.finditer(html))
    print(f"  Found {len(matches)} period rows in HackMD")
    for m in matches:
        period = m.group(1)
        funbox_id = m.group(2)
        b4_id = m.group(3)
        period_norm = re.sub(r'(\d{4})[-年](\d{1,2})[、/](\d{1,2})月',
                            r'\1-\2、\3月', period)
        results.append({
            "period": period_norm,
            "funbox_id": funbox_id,
            "b4_id": b4_id,
        })
        print(f"    {period_norm}: Funbox={funbox_id[:16]}..., B4={b4_id[:16]}...")
    return results


def get_source_name(source_type: str, period: str) -> str:
    """Get formatted source name."""
    return f"{source_type} ({period})"


def main():
    # Load existing events.json as fallback for periods where HackMD fails
    existing_events = []
    existing_sources = []
    try:
        with open("events.json", "r") as f:
            existing_data = json.load(f)
            existing_events = existing_data.get("events", [])
            existing_sources = existing_data.get("sources", [])
            print(f"Loaded {len(existing_events)} events from existing events.json")
            # Index existing events by source
            existing_by_source = {}
            for e in existing_events:
                src = e["source"]
                if src not in existing_by_source:
                    existing_by_source[src] = []
                existing_by_source[src].append(e)
    except Exception as e:
        print(f"  No existing events.json found: {e}")
        existing_by_source = {}
    
    # Fetch HackMD
    print("Fetching HackMD index page...")
    try:
        hackmd_html = fetch_url(HACKMD_URL)
        hackmd_rows = parse_hackmd_all_sheets(hackmd_html)
    except Exception as e:
        print(f"HackMD fetch failed: {e}")
        hackmd_rows = []
    
    all_events = []
    all_sources = []
    sources_with_new_data = set()
    
    # Process each period from HackMD
    for row in hackmd_rows:
        period = row["period"]
        
        # Try Funbox
        funbox_name = get_source_name("Funbox門市G3", period)
        funbox_events, funbox_err = fetch_sheet(funbox_name, row["funbox_id"])
        if funbox_events:
            all_events.extend(funbox_events)
            all_sources.append({"name": funbox_name, "sheet_id": row["funbox_id"]})
            sources_with_new_data.add(funbox_name)
            print(f"  ✓ {funbox_name}: {len(funbox_events)} events")
        else:
            # Fallback: keep existing events for this source
            existing = [e for e in existing_events if e["source"] == funbox_name]
            if existing:
                all_events.extend(existing)
                all_sources.append({"name": funbox_name, "sheet_id": row["funbox_id"], "source": "existing"})
                print(f"  ~ {funbox_name}: kept {len(existing)} existing events (HackMD failed: {funbox_err})")
            else:
                print(f"  ✗ {funbox_name}: no events, no fallback (error: {funbox_err})")
        
        # Try B4
        b4_name = get_source_name("B4合作據點G3", period)
        b4_events, b4_err = fetch_sheet(b4_name, row["b4_id"])
        
        # Detect if B4 URL is wrong: if it returns nearly the same count as Funbox,
        # it means Funbox and B4 are pointing to the same sheet (HackMD error)
        b4_is_wrong = (len(b4_events) > 0 and len(b4_events) == len(funbox_events))
        
        if b4_events and not b4_is_wrong:
            all_events.extend(b4_events)
            all_sources.append({"name": b4_name, "sheet_id": row["b4_id"]})
            sources_with_new_data.add(b4_name)
            print(f"  ✓ {b4_name}: {len(b4_events)} events")
        else:
            if b4_is_wrong:
                print(f"  B4 URL returns same as Funbox ({len(b4_events)} events) -- likely HackMD error, trying direct link...")
            # Fallback 1: known good direct link
            direct_link_used = False
            if period in KNOWN_GOOD_LINKS and b4_name in KNOWN_GOOD_LINKS[period]:
                direct_id = KNOWN_GOOD_LINKS[period][b4_name]
                print(f"  Trying known good direct link for {period}...")
                b4_events2, b4_err2 = fetch_sheet(b4_name, direct_id)
                if b4_events2:
                    all_events.extend(b4_events2)
                    all_sources.append({"name": b4_name, "sheet_id": direct_id, "source": "known_good_direct"})
                    sources_with_new_data.add(b4_name)
                    print(f"  ✓ {b4_name} (direct): {len(b4_events2)} events")
                    direct_link_used = True
            
            if not direct_link_used:
                # Fallback 2: keep existing events for this source
                existing = [e for e in existing_events if e["source"] == b4_name]
                if existing:
                    all_events.extend(existing)
                    all_sources.append({"name": b4_name, "sheet_id": row["b4_id"], "source": "existing"})
                    print(f"  ~ {b4_name}: kept {len(existing)} existing events")
                else:
                    err_msg = b4_err if b4_is_wrong else f"no events (error: {b4_err})"
                    print(f"  ✗ {b4_name}: {err_msg}")
    
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
    
    # Sort and assign IDs
    deduped.sort(key=lambda e: e["date"], reverse=True)
    for i, event in enumerate(deduped, 1):
        prefix = "funbox" if "Funbox" in event["source"] else "b4"
        event["id"] = f"{prefix}-{i:03d}"
    
    output = {
        "lastUpdated": date.today().isoformat(),
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
