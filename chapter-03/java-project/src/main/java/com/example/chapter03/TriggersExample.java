package com.example.chapter03;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Based on: "Event-Time Triggers with Early and Late Firings (Java)".
 *
 * DirectRunner won't perfectly emulate late data/watermark behavior, but this shows the correct Beam config.
 */
public class TriggersExample {

  public static void run() {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    long base = Instant.parse("2026-02-14T00:00:00Z").getMillis();

    var events =
        p.apply("ReadEvents",
            Create.of(Arrays.asList(
                new Event("u1", "click", base +  10_000, 1),
                new Event("u1", "click", base +  20_000, 1),
                new Event("u1", "click", base + 400_000, 1))));

    var withTs =
        events.apply("AssignEventTime",
            WithTimestamps.of(e -> new Instant(e.getTimestampMillis()))
                .withAllowedTimestampSkew(Duration.ZERO));

    var keyed = withTs.apply("KeyByUser", WithKeys.of(Event::getUserId))
                      .setCoder(KV.getCoder(org.apache.beam.sdk.coders.StringUtf8Coder.of(), withTs.getCoder()));

    var windowed =
        keyed.apply("Window+Triggers",
            Window.<KV<String, Event>>into(FixedWindows.of(Duration.standardMinutes(5)))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(1)))
                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(Duration.standardMinutes(5))
                .accumulatingFiredPanes());

    var counts =
        windowed.apply("CountPerKey", Combine.perKey(iter -> {
          long c = 0;
          for (Event e : iter) c++;
          return c;
        }));

    counts.apply("ToString",
            MapElements.into(TypeDescriptors.strings()).via(kv -> "[TRIGGERED] " + kv.getKey() + " -> " + kv.getValue()))
          .apply(Log.of("Triggers"));

    p.run().waitUntilFinish();
  }
}
