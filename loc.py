import requests
import pandas as pd
import os

API_KEY = os.environ["API_KEY"]  # Replace with your OpenAQ v3 key
BASE_URL = "https://api.openaq.org/v3"


def fetch_locations(parameter_id=None, country_iso=None, limit=100, page=1):
    """
    Fetch locations from OpenAQ v3 filtered by parameter and/or country.
    """
    url = f"{BASE_URL}/locations"
    headers = {"X-API-Key": API_KEY}
    params = {
        "limit": limit,
        "page": page
    }
    if parameter_id is not None:
        params["parameters_id"] = parameter_id
    if country_iso is not None:
        params["iso"] = country_iso

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def parse_locations(json_data):
    """
    Extract useful fields from locations JSON into flat records.
    """
    records = []
    for loc in json_data.get("results", []):
        coord = loc.get("coordinates", {})
        country = loc.get("country", {})
        sensors = loc.get("sensors", [])
        measured_params = [s.get("parameter", {}).get("name") for s in sensors
                           if s.get("parameter")]

        # Protect against None values
        datetime_first = loc.get("datetimeFirst") or {}
        datetime_last = loc.get("datetimeLast") or {}

        records.append({
            "location_id": loc.get("id"),
            "name": loc.get("name"),
            "locality": loc.get("locality"),
            "timezone": loc.get("timezone"),
            "country_code": country.get("code"),
            "country_name": country.get("name"),
            "latitude": coord.get("latitude"),
            "longitude": coord.get("longitude"),
            "is_mobile": loc.get("isMobile"),
            "is_monitor": loc.get("isMonitor"),
            "measured_parameters": ",".join(measured_params),
            "datetime_first_utc": datetime_first.get("utc"),
            "datetime_last_utc": datetime_last.get("utc")
        })
    return records


def main():
    # Fetch ALL locations (no filters)
    parameter_id = None  # remove parameter filter
    country_iso = None  # remove country filter

    all_records = []
    page = 1
    per_page = 100  # maximum allowed

    while True:
        data = fetch_locations(parameter_id=parameter_id,
                               country_iso=country_iso, limit=per_page,
                               page=page)
        recs = parse_locations(data)
        if not recs:
            break
        all_records.extend(recs)
        print(f"Fetched page {page} — {len(recs)} locations")
        page += 1
        # Stop when we reach the last page
        if page > data.get("meta", {}).get("pageCount", page):
            break

    df = pd.DataFrame(all_records)
    if not df.empty:
        df.to_csv("openaq_locations_all.csv", index=False)
        print(f"✅ Saved {len(df)} locations to openaq_locations_all.csv")
        print(df.head())
    else:
        print("⚠️ No location data found.")


if __name__ == "__main__":
    main()
