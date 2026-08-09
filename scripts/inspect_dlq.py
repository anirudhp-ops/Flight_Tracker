#!/usr/bin/env python3
"""
Prints every message currently on dead-letter-events in a human-readable
form: event key, where it originally failed, why, and when. Read-only —
does not consume/commit offsets against a real consumer group, so running
this doesn't affect anything else reading the topic.

Usage:
    python scripts/inspect_dlq.py                # all DLQ events
    python scripts/inspect_dlq.py --since-hours 1 # only the last hour
    python scripts/inspect_dlq.py --limit 20
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flight_tracker.events.dlq_utils import fetch_dlq_events


def print_report(events: list[dict]) -> None:
    if not events:
        print("dead-letter-events is empty (within the requested window).")
        return

    print(f"{len(events)} dead-letter event(s):\n")
    for i, e in enumerate(events, 1):
        print(f"--- [{i}] {e.get('failed_at', '?')} ---")
        print(f"  consumer group : {e.get('consumer_group', '?')}")
        print(f"  original topic : {e.get('original_topic', '?')}[{e.get('original_partition', '?')}]"
              f"@{e.get('original_offset', '?')}")
        print(f"  key            : {e.get('key', '?')}")
        print(f"  error          : {e.get('error_type', '?')}: {e.get('error', '?')}")
        value = e.get("value")
        if value:
            preview = value if len(value) <= 200 else value[:200] + "...(truncated)"
            print(f"  value preview  : {preview}")
        print()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-hours", type=float, default=None, help="Only show failures from the last N hours")
    parser.add_argument("--limit", type=int, default=None, help="Only show the N most recent failures")
    args = parser.parse_args()

    events = asyncio.run(fetch_dlq_events(args.since_hours, args.limit))
    print_report(events)


if __name__ == "__main__":
    main()
