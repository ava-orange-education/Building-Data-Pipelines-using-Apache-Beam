package com.beam.ch03;

import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class EventTimeAssignment {

  // Example Event type used in the chapter narrative
  public static class Event {
    private final long timestampMillis;

    public Event(long timestampMillis) {
      this.timestampMillis = timestampMillis;
    }

    public long getTimestamp() {
      return timestampMillis;
    }
  }

  public static PCollection<Event> assignEventTime(PCollection<Event> events) {
    return events.apply("AssignEventTime",
        WithTimestamps.of((Event e) -> new Instant(e.getTimestamp()))
            .withAllowedTimestampSkew(Duration.ZERO));
  }
}