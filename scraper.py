#!/usr/bin/env python3
"""
戰鬥陀螺比賽資訊爬蟲
資料來源：HackMD 索引頁 → Google Sheets CSV export
"""

import csv
import io
import json
import re
import sys
import urllib.request
from datetime import datetime, date


HACKMD_URL = "https://hackmd.io/@liangyutw/beyblade-important-record"
SHEETS_CSV_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

# Fallback: known sheet IDs (used when HackMD parsing fails)
FALLBACK_SHEETS = {
    "Funbox門市G3": "1X5qc079OSSXzoX20VcIHlsi444kdhVBH58grf3-TBiY",
    "B4合作據點G3": "1jgw-YH4-NQ4omxhuddlHqUD5N4ZqEJMbgmxsuurHCIM",
}

CITIES = [
    "台北市", "新北市", "桃園市", "台中市", "台南市", "高雄市",
    "基隆市", "新竹市", "新竹縣", "苗栗縣", "彰化縣", "南投縣",
    "雲林縣", "嘉義市", "嘉義縣", "屏東縣", "宜蘭縣", "花蓮縣",
    "台東縣", "澎湖縣", "金門縣", "連江縣",
    "彰化市", "竹北市",
]

CITY_NORMALIZE = {
    "臺北市": "台北市", "臺南市": "台南市", "臺中市": "台中市",
    "花蓮市": "花蓮縣", "屏東市": "屏東縣", "彰化市": "彰化縣",
    "宜蘭市": "宜蘭縣", "竹北市": "新竹縣",
}

# District → city mapping for addresses missing city prefix
DISTRICT_TO_CITY = {
    "桃園區": "桃園市", "中壢區": "桃園市", "板橋區": "新北市",
    "三重區": "新北市", "新莊區": "新北市", "中和區": "新北市",
    "永和區": "新北市", "新店區": "新北市", "樹林區": "新北市",
}

MAX_RETRIES = 3


def fetch_url(url: str) -> str:
    """Fetch URL with retry logic."""
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
                print(f"  Retry {attempt + 1}/{MAX_RETRIES}: {e}")
    raise last_err


def extract_city(address: str) -> str:
    """Extract city/county from address."""
    # Strip leading zip code
    addr = re.sub(r'^\d{3,6}', '', address)
    for city in CITIES:
        if city in addr:
            return CITY_NORMALIZE.get(city, city)
    if len(addr) >= 3 and addr[2] in ("市", "縣"):
        raw = addr[:3]
        return CITY_NORMALIZE.get(raw, raw)
    # Short form: 北市 → 台北市
    if addr.startswith("北市"):
        return "台北市"
    # District-based fallback
    for dist, city in DISTRICT_TO_CITY.items():
        if addr.startswith(dist):
            return city
    # Try match with zip code prefix (e.g. 807高雄市)
    m = re.search(r'\d{3}([^\d]{2}[市縣])', address)
    if m:
        raw = m.group(1)
        return CITY_NORMALIZE.get(raw, raw)
    return ""


def parse_hackmd_for_latest_sheets(html: str) -> dict:
    """Parse HackMD page to find the latest pair of Google Sheets IDs.
    
    The HackMD page has a table with rows like:
    | 2026-3、4月 | [G3](https://docs.google.com/.../SHEET_ID/...) | [B4](https://docs.google.com/.../SHEET_ID/...) |
    
    We find the last (most recent) row with two sheet links.
    """
    # Find table rows with sheet links
    # Pattern: period text + two Google Sheets URLs
    row_pattern = re.compile(
        r'\|\s*(\d{4}-\d{1,2}[、,]\d{1,2}月)\s*\|'
        r'\s*\[G3\]\(https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)'
        r'[^|]*\|\s*\[B4\]\(https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)'
    )
    
    matches = list(row_pattern.finditer(html))
    if not matches:
        return {}
    
    # Take the last match (most recent period)
    last = matches[-1]
    period = last.group(1)
    funbox_id = last.group(2)
    b4_id = last.group(3)
    
    print(f"  HackMD: found latest period '{period}'")
    print(f"    Funbox G3: {funbox_id[:16]}...")
    print(f"    B4 G3:     {b4_id[:16]}...")
    
    return {
        "period": period,
        "Funbox門市G3": funbox_id,
        "B4合作據點G3": b4_id,
    }


def determine_year(month: int) -> int:
    """Determine year for a month-only date.
    
    If the month is far behind current month (>6 months ago),
    assume it's next year's event.
    """
    now = datetime.now()
    if month < now.month - 6:
        return now.year + 1
    return now.year


def parse_date(date_str: str) -> str:
    """Convert '3/15' or '2026/3/15' to '2026-03-15'."""
    date_str = date_str.strip()
    if not date_str:
        return ""
    # Already ISO format?
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    parts = date_str.replace("月", "/").replace("日", "").split("/")
    try:
        if len(parts) == 3:
            # Format: YYYY/M/D or M/D/YYYY
            a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
            if a > 100:  # YYYY/M/D
                return f"{a}-{b:02d}-{c:02d}"
            elif c > 100:  # M/D/YYYY
                return f"{c}-{a:02d}-{b:02d}"
        elif len(parts) == 2:
            # Format: M/D (no year)
            month, day = int(parts[0]), int(parts[1])
            year = determine_year(month)
            return f"{year}-{month:02d}-{day:02d}"
    except (ValueError, IndexError):
        pass
    return ""


def parse_time(time_str: str) -> str:
    """Normalize time string."""
    time_str = time_str.strip()
    if not time_str:
        return ""
    time_str = time_str.replace("：", ":").strip()
    if re.match(r'^\d{1,2}:\d{2}$', time_str):
        return time_str
    # Try to extract HH:MM from longer strings
    m = re.search(r'(\d{1,2}:\d{2})', time_str)
    if m:
        return m.group(1)
    return ""


def parse_capacity(cap_str: str) -> int:
    """Parse capacity, return 0 if invalid."""
    cap_str = str(cap_str).strip()
    # Extract digits only
    digits = re.sub(r'[^\d]', '', cap_str)
    if digits:
        try:
            return int(digits)
        except ValueError:
            pass
    return 0


def parse_csv_sheet(csv_text: str, source_name: str) -> list:
    """Parse a CSV sheet into event dicts."""
    events = []
    reader = csv.reader(io.StringIO(csv_text))
    
    header_found = False
    skipped = 0
    for row in reader:
        if not row or all(c.strip() == "" for c in row):
            continue
        
        # Find header row
        if not header_found:
            row_str = "".join(row)
            if "店家名稱" in row_str:
                header_found = True
            continue
        
        # Data row - need at least 9 columns
        if len(row) < 9:
            skipped += 1
            continue
        
        seq, store, phone, address, event_date, event_time, capacity, reg_method, category = row[:9]
        
        store = store.strip()
        if not store:
            continue
        
        # Skip note/disclaimer rows
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
    
    if not header_found:
        print(f"  WARNING: No header row found in {source_name}!")
    if skipped > 0:
        print(f"  Skipped {skipped} malformed rows")
    
    return events


def main():
    all_events = []
    sources = []
    period = "unknown"
    
    # Step 1: Try to discover latest sheets from HackMD
    sheet_map = None
    try:
        print("Fetching HackMD index page...")
        hackmd_html = fetch_url(HACKMD_URL)
        result = parse_hackmd_for_latest_sheets(hackmd_html)
        if result:
            period = result.pop("period")
            sheet_map = result
            print(f"  Using HackMD-discovered sheets for '{period}'")
    except Exception as e:
        print(f"  HackMD fetch failed: {e}")
    
    # Step 2: Fallback to known sheets
    if not sheet_map:
        print("  Falling back to known sheet IDs")
        sheet_map = FALLBACK_SHEETS
        period = "fallback"
    
    # Step 3: Download and parse each sheet
    for source_name, sheet_id in sheet_map.items():
        url = SHEETS_CSV_URL.format(sheet_id=sheet_id)
        print(f"Fetching {source_name} ({sheet_id[:12]}...)...")
        try:
            csv_text = fetch_url(url)
            events = parse_csv_sheet(csv_text, source_name)
            print(f"  Parsed {len(events)} events")
            all_events.extend(events)
            sources.append({
                "name": source_name,
                "period": period,
            })
        except Exception as e:
            print(f"  ERROR fetching {source_name}: {e}")
    
    # Step 4: Validate
    if not all_events:
        print("\nERROR: No events parsed! Check sheet structure or network.")
        sys.exit(1)
    
    # Step 5: Sort and assign IDs
    all_events.sort(key=lambda e: e["date"], reverse=True)
    for i, event in enumerate(all_events, 1):
        prefix = "funbox" if "Funbox" in event["source"] else "b4"
        event["id"] = f"{prefix}-{i:03d}"
    
    output = {
        "lastUpdated": date.today().isoformat(),
        "sources": sources,
        "events": all_events,
    }
    
    out_path = "events.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\nDone! {len(all_events)} events written to {out_path}")
    
    # Stats
    cities = {}
    for e in all_events:
        c = e["city"] or "未知"
        cities[c] = cities.get(c, 0) + 1
    print("\n各縣市比賽數：")
    for c, n in sorted(cities.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")


if __name__ == "__main__":
    main()
