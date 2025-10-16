import requests
import pandas as pd
import os

API_KEY = os.environ["API_KEY"]
BASE_URL = "https://api.openaq.org/v3"

ROW_LIMIT = 20000  # max rows

# Map pollutants to their IDs
POLLUTANT_IDS = {
    "pm25": 2,
    "pm10": 1,
    "no2": 5,
    "o3": 7,
    "co": 4
}


def fetch_latest(param_id, limit=100, page=1):
    url = f"{BASE_URL}/parameters/{param_id}/latest"
    headers = {"X-API-Key": API_KEY}
    params = {"limit": limit, "page": page}
    resp = requests.get(url, headers=headers, params=params)

    # ✅ If API has no such page, just return {}
    if resp.status_code == 404:
        return {}

    resp.raise_for_status()
    return resp.json()


def parse_latest(json_data, param_id, param_name):
    records = []
    for entry in json_data.get("results", []):
        dt = entry.get("datetime", {})
        coord = entry.get("coordinates", {})
        records.append({
            "parameter_id": param_id,
            "parameter_name": param_name,
            "value": entry.get("value"),
            "latitude": coord.get("latitude"),
            "longitude": coord.get("longitude"),
            "datetime_utc": dt.get("utc"),
            "datetime_local": dt.get("local"),
            "locationsId": entry.get("locationsId"),
            "sensorsId": entry.get("sensorsId"),
            "country": entry.get("country"),
            "city": entry.get("city")
        })
    return records


def collect_pollutant(pollutant):
    if pollutant not in POLLUTANT_IDS:
        print(f"❌ Invalid pollutant: {pollutant}")
        return

    param_id = POLLUTANT_IDS[pollutant]
    print(f"\n🔎 Fetching up to {ROW_LIMIT} rows for {pollutant.upper()}...")

    all_records = []
    page = 1
    while True:
        if len(all_records) >= ROW_LIMIT:
            print(f"⚠️ Stopping: reached {ROW_LIMIT} rows.")
            break

        data = fetch_latest(param_id, limit=100, page=page)
        if not data:  # ✅ 404 or empty result
            print(f"⚠️ No more data available (page {page}).")
            break

        recs = parse_latest(data, param_id, pollutant)
        if not recs:  # ✅ API returned empty results
            print(f"⚠️ No records on page {page}, stopping.")
            break

        all_records.extend(recs)
        print(
            f"  ✅ Page {page} -> {len(recs)} records (total: {len(all_records)})")
        page += 1

    df = pd.DataFrame(all_records[:ROW_LIMIT])
    if not df.empty:
        filename = f"pollution_{pollutant}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved {len(df)} rows to {filename}")
    else:
        print("⚠️ No data returned.")


def main():
    pollutant = "co"  # choose from: pm25, pm10, no2, o3, co
    collect_pollutant(pollutant)


if __name__ == "__main__":
    main()
