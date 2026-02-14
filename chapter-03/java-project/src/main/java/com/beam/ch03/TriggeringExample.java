package com.beam.ch03;

import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class TriggeringExample {

  public static <T> PCollection<T> windowWithTriggers(PCollection<T> events) {
    return events.apply("Window+Triggers",
        Window.<T>into(FixedWindows.of(Duration.standardMinutes(5)))
            .triggering(
                AfterWatermark.pastEndOfWindow()
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardMinutes(1)))
                    .withLateFirings(AfterPane.elementCountAtLeast(1))
            )
            .withAllowedLateness(Duration.standardMinutes(5))
            .accumulatingFiredPanes()
    );
  }
}