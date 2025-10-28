import requests
import json
import math
import csv
import time
from datetime import datetime

# ---------------------------
# CONFIG
# ---------------------------
import os

CLIENT_ID = os.environ["NETATMO_CLIENT_ID"]
CLIENT_SECRET = os.environ["NETATMO_CLIENT_SECRET"]
USERNAME = os.environ["NETATMO_USERNAME"]
PASSWORD = os.environ["NETATMO_PASSWORD"]

TOKEN_FILE = "netatmo_token.json"

CENTER_LAT = 45.7740
CENTER_LON = 4.8050
BOX_RADIUS_M = 100  # meters
VARIABLES = ["temperature", "humidity", "pressure"]

POLL_INTERVAL = 15 * 60  # seconds (15 minutes)
CSV_FILE = "public_stations_history.csv"

# ---------------------------
# Get access token programmatically
# ---------------------------
def get_access_token():
    try:
        with open(TOKEN_FILE, "r") as f:
            token_data = json.load(f)
    except FileNotFoundError:
        token_data = None

    if token_data:
        expires_at = token_data.get("expire_in", 0) + token_data.get("obtained_at", 0)
        if expires_at > int(time.time()):
            return token_data["access_token"]

    data = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
        "scope": "read_station"
    }

    resp = requests.post("https://api.netatmo.com/oauth2/token", data=data)
    resp.raise_for_status()
    token_data = resp.json()
    token_data["obtained_at"] = int(time.time())

    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f)

    return token_data["access_token"]

# ---------------------------
# Calculate bounding box in meters
# ---------------------------
def calculate_bbox(center_lat, center_lon, radius_m):
    delta_lat = radius_m / 111000
    delta_lon = radius_m / (111000 * math.cos(math.radians(center_lat)))
    return (
        round(center_lat + delta_lat, 6),
        round(center_lon + delta_lon, 6),
        round(center_lat - delta_lat, 6),
        round(center_lon - delta_lon, 6)
    )

# ---------------------------
# Fetch public stations
# ---------------------------
def get_public_data_for_var(access_token, center_lat, center_lon, radius_m, var):
    lat_ne, lon_ne, lat_sw, lon_sw = calculate_bbox(center_lat, center_lon, radius_m)
    url = "https://api.netatmo.com/api/getpublicdata"
    params = {
        "lat_ne": lat_ne,
        "lon_ne": lon_ne,
        "lat_sw": lat_sw,
        "lon_sw": lon_sw,
        "required_data": var,
        "filter": "false"
    }
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json().get("body", [])

# ---------------------------
# Parse station data
# ---------------------------
def parse_station_data(stations):
    all_data = []
    timestamp_poll = int(time.time())
    for st in stations:
        station_id = st.get("_id")
        loc = st.get("place", {}).get("location", [])
        city = st.get("place", {}).get("city", "")
        country = st.get("place", {}).get("country", "")
        altitude = st.get("place", {}).get("altitude", None)
        measures = st.get("measures", {})

        for module_id, module_data in measures.items():
            types = module_data.get("type", [])
            for ts, values in module_data.get("res", {}).items():
                row = {
                    "poll_time": datetime.fromtimestamp(timestamp_poll).isoformat(),
                    "station_id": station_id,
                    "module_id": module_id,
                    "timestamp": ts,
                    "latitude": loc[1] if loc else None,
                    "longitude": loc[0] if loc else None,
                    "city": city,
                    "country": country,
                    "altitude": altitude
                }
                for t, v in zip(types, values):
                    row[t] = v
                all_data.append(row)
    return all_data

# ---------------------------
# Save to CSV (append)
# ---------------------------
def save_to_csv(data, filename=CSV_FILE):
    if not data:
        print("No data to save.")
        return
    keys = set()
    for row in data:
        keys.update(row.keys())
    keys = sorted(keys)
    # Append if file exists
    try:
        with open(filename, "r", encoding="utf-8") as f:
            existing = True
    except FileNotFoundError:
        existing = False

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not existing:
            writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} rows to {filename}")

# ---------------------------
# Main polling loop
# ---------------------------
if __name__ == "__main__":
    token = get_access_token()
    print("Starting polling every 15 minutes...")

    try:
        while True:
            all_parsed_data = []
            for var in VARIABLES:
                stations = get_public_data_for_var(token, CENTER_LAT, CENTER_LON, BOX_RADIUS_M, var)
                parsed = parse_station_data(stations)
                all_parsed_data.extend(parsed)

            if all_parsed_data:
                save_to_csv(all_parsed_data)
            else:
                print(f"{datetime.now()}: No stations found.")

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("Polling stopped by user.")
