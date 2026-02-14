package com.example.chapter03;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Implements the Chapter 3 windowing snippets:
 * - Fixed windows (tumbling)
 * - Sliding windows
 * - Session windows
 */
public class WindowingExamples {

  public static void run() {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    long base = Instant.parse("2026-02-14T00:00:00Z").getMillis();

    var events =
        p.apply("ReadEvents",
            Create.of(Arrays.asList(
                new Event("u1", "click", base +  10_000, 1),
                new Event("u1", "click", base + 200_000, 1),
                new Event("u1", "click", base + 320_000, 1),
                new Event("u2", "view",  base +  50_000, 1),
                new Event("u2", "view",  base + 400_000, 1))));

    var withTs = events.apply("AssignEventTime",
        org.apache.beam.sdk.transforms.WithTimestamps.of(e -> new Instant(e.getTimestampMillis())));

    var keyed = withTs.apply("KeyByUser", WithKeys.of(Event::getUserId))
                      .setCoder(KV.getCoder(org.apache.beam.sdk.coders.StringUtf8Coder.of(), withTs.getCoder()));

    fixedWindowCount(keyed);
    slidingWindowCount(keyed);
    sessionWindowCount(keyed);

    p.run().waitUntilFinish();
  }

  private static void fixedWindowCount(org.apache.beam.sdk.values.PCollection<KV<String, Event>> keyed) {
    var counts =
        keyed.apply("FixedWindow5m", Window.into(FixedWindows.of(Duration.standardMinutes(5))))
             .apply("CountPerKeyFixed", Combine.perKey(evs -> {
               long c = 0;
               for (Event e : evs) c++;
               return c;
             }));

    counts.apply("FixedToString",
            MapElements.into(TypeDescriptors.strings()).via(kv -> "[FIXED] " + kv.getKey() + " -> " + kv.getValue()))
          .apply(Log.of("FixedWindows"));
  }

  private static void slidingWindowCount(org.apache.beam.sdk.values.PCollection<KV<String, Event>> keyed) {
    var counts =
        keyed.apply("Sliding10mEvery5m",
                Window.into(SlidingWindows.of(Duration.standardMinutes(10))
                                          .every(Duration.standardMinutes(5))))
             .apply("CountPerKeySliding", Combine.perKey(evs -> {
               long c = 0;
               for (Event e : evs) c++;
               return c;
             }));

    counts.apply("SlidingToString",
            MapElements.into(TypeDescriptors.strings()).via(kv -> "[SLIDING] " + kv.getKey() + " -> " + kv.getValue()))
          .apply(Log.of("SlidingWindows"));
  }

  private static void sessionWindowCount(org.apache.beam.sdk.values.PCollection<KV<String, Event>> keyed) {
    var counts =
        keyed.apply("SessionGap30m", Window.into(Sessions.withGapDuration(Duration.standardMinutes(30))))
             .apply("CountPerKeySession", Combine.perKey(evs -> {
               long c = 0;
               for (Event e : evs) c++;
               return c;
             }));

    counts.apply("SessionToString",
            MapElements.into(TypeDescriptors.strings()).via(kv -> "[SESSION] " + kv.getKey() + " -> " + kv.getValue()))
          .apply(Log.of("SessionWindows"));
  }
}
