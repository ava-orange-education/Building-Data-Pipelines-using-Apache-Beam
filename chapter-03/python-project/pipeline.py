import apache_beam as beam
from apache_beam import window, trigger


def assign_event_time(events):
    # Assuming each event is a dict with a 'timestamp' field (Unix epoch seconds)
    return events | "AssignEventTime" >> beam.Map(
        lambda e: window.TimestampedValue(e, e["timestamp"])
    )


def fixed_windows(events):
    # 5 minutes = 300 seconds
    return events | "Fixed5Min" >> beam.WindowInto(window.FixedWindows(300))


def sliding_windows(events):
    # 10 minutes window (600s) sliding every 5 minutes (300s)
    return events | "Sliding10MinEvery5Min" >> beam.WindowInto(window.SlidingWindows(600, 300))


def session_windows(events):
    # 30-minute gap = 1800 seconds
    return events | "Sessions30MinGap" >> beam.WindowInto(window.Sessions(1800))


def window_with_triggers(events):
    return events | "Window+Triggers" >> beam.WindowInto(
        window.FixedWindows(300),  # 5 minutes
        trigger=trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(60),  # early after 60s from first element
            late=trigger.AfterCount(1),             # late fire when 1 late element arrives
        ),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        allowed_lateness=300,  # 5 minutes
    )