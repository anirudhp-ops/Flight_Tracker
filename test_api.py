import httpx
import os
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timezone
from flight_tracker.ingestion.client import _raw_flight_to_event

load_dotenv()

async def main():
    live_api_enabled = os.getenv("ENABLE_FLIGHTAWARE_API", "false").lower() in {
        "1", "true", "yes", "on"
    }
    api_key = os.getenv("FLIGHTAWARE_API_KEY")
    if not live_api_enabled:
        raise SystemExit(
            "Paid FlightAware API calls are disabled. "
            "Set ENABLE_FLIGHTAWARE_API=true only when you intend to incur usage."
        )
    if not api_key or api_key == "YOUR_API_KEY":
        raise SystemExit("FLIGHTAWARE_API_KEY is not configured.")

    # Keep optional database dependencies out of the safe, disabled path.
    from flight_tracker.db.writer import create_pool, ensure_schema, write_events

    airport = os.getenv("TARGET_AIRPORT", "KSFO")
    url = f"https://aeroapi.flightaware.com/aeroapi/airports/{airport}/flights"
    response = httpx.get(url, headers={"x-apikey": api_key}, timeout=15)
    response.raise_for_status()
    data = response.json()

    flights = []
    for raw in (data.get("arrivals", []) + data.get("departures", []))[:5]:
        event = _raw_flight_to_event(raw, datetime.now(timezone.utc))
        if event:
            flights.append(event)

    pool = await create_pool("localhost", 5432, "flight_tracker", os.getenv("DB_USER", "postgres"), os.getenv("DB_PASSWORD", "postgres"))
    await ensure_schema(pool)
    written = await write_events(pool, flights)
    print(f"Written {written} flights to DB")
    await pool.close()

asyncio.run(main())
