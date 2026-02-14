package com.example.chapter02;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Runnable demo for SessionizationDoFn.
 * Writes session summaries when 30 minutes of event-time inactivity elapses per user.
 */
public class SessionizationPipeline {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

    Instant t0 = Instant.parse("2020-01-01T00:00:00Z");

    // userA has two clicks within 30 minutes => one session
    // userB has a long gap => two sessions
    p.apply(
            Create.of(
                TimestampedValue.of(
                    KV.of("userA", new SessionizationDoFn.ClickEvent("/home", t0.plus(Duration.standardMinutes(1)))),
                    t0.plus(Duration.standardMinutes(1))),
                TimestampedValue.of(
                    KV.of("userA", new SessionizationDoFn.ClickEvent("/pricing", t0.plus(Duration.standardMinutes(10)))),
                    t0.plus(Duration.standardMinutes(10))),
                TimestampedValue.of(
                    KV.of("userB", new SessionizationDoFn.ClickEvent("/home", t0.plus(Duration.standardMinutes(2)))),
                    t0.plus(Duration.standardMinutes(2))),
                TimestampedValue.of(
                    KV.of("userB", new SessionizationDoFn.ClickEvent("/about", t0.plus(Duration.standardMinutes(80)))),
                    t0.plus(Duration.standardMinutes(80)))))
        .apply("Sessionize", ParDo.of(new SessionizationDoFn()))
        .apply(
            "ToString",
            MapElements.into(TypeDescriptors.strings())
                .via(s -> s.toString()))
        .apply(TextIO.write().to("sessions-output").withSuffix(".txt").withoutSharding());

    p.run().waitUntilFinish();
  }
}
