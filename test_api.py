import httpx
import os
from dotenv import load_dotenv
from flight_tracker.ingestion.client import _raw_flight_to_event
from datetime import datetime, timezone

load_dotenv()

api_key = os.getenv("FLIGHTAWARE_API_KEY")
airport = os.getenv("TARGET_AIRPORT", "KSFO")

url = f"https://aeroapi.flightaware.com/aeroapi/airports/{airport}/flights"
response = httpx.get(url, headers={"x-apikey": api_key})
data = response.json()

first_flight = data["arrivals"][0]

try:
    event = _raw_flight_to_event(first_flight, datetime.now(timezone.utc))
    print(event)
except Exception as e:
    print("Error:", e)