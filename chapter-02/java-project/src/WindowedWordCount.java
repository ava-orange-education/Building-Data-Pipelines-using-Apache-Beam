package com.beam.stateful;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.coders.LongCoder;
import org.apache.beam.sdk.state.StateId;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpecs;
import org.joda.time.Duration;

public class WindowedWordCount {

  public static DoFn<KV<String, String>, KV<String, Long>> getDoFn() {
    return new DoFn<KV<String, String>, KV<String, Long>>() {

      @StateId("count")
      private final StateSpec<CombiningState<Long, long[], Long>> countSpec =
          StateSpecs.combiningValue(LongCoder.of(), org.apache.beam.sdk.transforms.Sum.ofLongs());

      @ProcessElement
      public void processElement(
          ProcessContext c,
          @StateId("count") CombiningState<Long, long[], Long> countState) {

        countState.add(1L);
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c) {
        // Output handled via triggers/window closure in real pipelines
      }
    };
  }
}