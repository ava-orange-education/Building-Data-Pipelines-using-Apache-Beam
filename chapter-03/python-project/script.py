"""Chapter 03 — Apache Beam (Python) examples

Implements the Chapter 3 snippets:
- Assign event-time timestamps using TimestampedValue
- Fixed windows, sliding windows, session windows
- Event-time triggers with early and late firings
"""

import time
import apache_beam as beam
from apache_beam import window, trigger
from apache_beam.options.pipeline_options import PipelineOptions


def sample_events(epoch_base: int):
    # each event is a dict with a 'timestamp' field (unix epoch seconds)
    return [
        {"user_id": "u1", "type": "click", "timestamp": epoch_base + 5, "value": 1},
        {"user_id": "u1", "type": "click", "timestamp": epoch_base + 65, "value": 1},
        {"user_id": "u2", "type": "view",  "timestamp": epoch_base + 125, "value": 1},
        {"user_id": "u1", "type": "click", "timestamp": epoch_base + 400, "value": 1},
    ]


def assign_event_time(events):
    # Snippet: window.TimestampedValue(e, e['timestamp'])
    return events | "AssignEventTime" >> beam.Map(lambda e: window.TimestampedValue(e, e["timestamp"]))


def run_fixed_sliding_session(pcoll):
    keyed = pcoll | "KeyByUser" >> beam.Map(lambda e: (e["user_id"], e))

    # Fixed window: 5 minutes (300 seconds)
    _ = (
        keyed
        | "FixedWindow5m" >> beam.WindowInto(beam.window.FixedWindows(300))
        | "CountFixed" >> beam.combiners.Count.PerKey()
        | "PrintFixed" >> beam.Map(lambda kv: print("[FIXED]", kv))
    )

    # Sliding window: 10 minutes sliding every 5 minutes
    _ = (
        keyed
        | "Sliding10mEvery5m" >> beam.WindowInto(beam.window.SlidingWindows(600, 300))
        | "CountSliding" >> beam.combiners.Count.PerKey()
        | "PrintSliding" >> beam.Map(lambda kv: print("[SLIDING]", kv))
    )

    # Session window: 30-minute gap (1800 seconds)
    _ = (
        keyed
        | "SessionGap30m" >> beam.WindowInto(beam.window.Sessions(1800))
        | "CountSession" >> beam.combiners.Count.PerKey()
        | "PrintSession" >> beam.Map(lambda kv: print("[SESSION]", kv))
    )


def run_triggers(pcoll):
    keyed = pcoll | "KeyByUserForTriggers" >> beam.Map(lambda e: (e["user_id"], e))

    # Snippet: AfterWatermark(early=AfterProcessingTime(60), late=AfterCount(1))
    windowed = (
        keyed
        | "TriggeredWindow" >> beam.WindowInto(
            beam.window.FixedWindows(300),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(60),
                late=trigger.AfterCount(1),
            ),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=300,
        )
    )

    _ = (
        windowed
        | "CountTriggered" >> beam.combiners.Count.PerKey()
        | "PrintTriggered" >> beam.Map(lambda kv: print("[TRIGGERED]", kv))
    )


def main():
    epoch_base = int(time.time())
    opts = PipelineOptions()

    with beam.Pipeline(options=opts) as p:
        events = p | "ReadEvents" >> beam.Create(sample_events(epoch_base))
        with_ts = assign_event_time(events)

        run_fixed_sliding_session(with_ts)
        run_triggers(with_ts)


if __name__ == "__main__":
    main()
