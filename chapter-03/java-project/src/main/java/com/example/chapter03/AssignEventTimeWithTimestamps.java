package com.example.chapter03;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Based on: "Assigning Event-Time Timestamps Using WithTimestamps (Java)".
 */
public class AssignEventTimeWithTimestamps {

  public static void run() {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    long base = Instant.parse("2026-02-14T00:00:00Z").getMillis();
    var events =
        p.apply("ReadEvents",
            Create.of(Arrays.asList(
                new Event("u1", "click", base + 5_000, 1),
                new Event("u1", "click", base + 62_000, 1),
                new Event("u2", "view",  base + 125_000, 1))));

    var eventsWithTs =
        events.apply("AssignEventTime",
            WithTimestamps.of((Event e) -> new Instant(e.getTimestampMillis()))
                .withAllowedTimestampSkew(Duration.ZERO));

    eventsWithTs
        .apply("ToString", MapElements.into(TypeDescriptors.strings()).via(Event::toString))
        .apply(Log.of("WithTimestamps"));

    p.run().waitUntilFinish();
  }
}
