package com.beam.ch03;

import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class WindowingExamples {

  public static <T> PCollection<T> fixedWindows(PCollection<T> events) {
    return events.apply("Fixed5Min",
        Window.into(FixedWindows.of(Duration.standardMinutes(5))));
  }

  public static <T> PCollection<T> slidingWindows(PCollection<T> events) {
    return events.apply("Sliding10MinEvery5Min",
        Window.into(SlidingWindows.of(Duration.standardMinutes(10))
            .every(Duration.standardMinutes(5))));
  }

  public static <T> PCollection<T> sessionWindows(PCollection<T> eventsByKeyOrUser) {
    return eventsByKeyOrUser.apply("Sessions30MinGap",
        Window.into(Sessions.withGapDuration(Duration.standardMinutes(30))));
  }
}