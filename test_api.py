import httpx
import os
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timezone
from flight_tracker.ingestion.client import _raw_flight_to_event
from flight_tracker.db.writer import create_pool, ensure_schema, write_events

load_dotenv()

api_key = os.getenv("FLIGHTAWARE_API_KEY")
airport = os.getenv("TARGET_AIRPORT", "KSFO")

url = f"https://aeroapi.flightaware.com/aeroapi/airports/{airport}/flights"
response = httpx.get(url, headers={"x-apikey": api_key})
data = response.json()

# Parse first 5 flights
flights = []
for raw in (data.get("arrivals", []) + data.get("departures", []))[:5]:
    event = _raw_flight_to_event(raw, datetime.now(timezone.utc))
    if event:
        flights.append(event)

async def main():
    pool = await create_pool("localhost", 5432, "flight_tracker", os.getenv("DB_USER", "postgres"), os.getenv("DB_PASSWORD", "postgres"))
    await ensure_schema(pool)
    written = await write_events(pool, flights)
    print(f"Written {written} flights to DB")
    await pool.close()

asyncio.run(main())