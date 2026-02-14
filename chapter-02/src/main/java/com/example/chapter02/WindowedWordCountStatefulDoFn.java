package com.example.chapter02;

import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateId;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerId;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimeDomain;
import org.apache.beam.sdk.values.KV;

/**
 * Per-key, per-window word count using CombiningState.
 *
 * Important detail: Beam scopes state/timers by (key, window). So this DoFn naturally keeps
 * independent counts per fixed window when applied after Window.into(FixedWindows...).
 */
public class WindowedWordCountStatefulDoFn extends DoFn<KV<String, String>, KV<String, Long>> {

  @StateId("count")
  private final StateSpec<CombiningState<Long, long[], Long>> countSpec =
      StateSpecs.combiningValue(VarLongCoder.of(), Sum.ofLongs());

  @TimerId("windowEnd")
  private final TimerSpec windowEndTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("count") CombiningState<Long, long[], Long> countState,
      @TimerId("windowEnd") Timer windowEnd) {

    // Add one occurrence for this word.
    countState.add(1L);

    // Ensure we emit once per window at the window's max timestamp.
    BoundedWindow w = c.window();
    windowEnd.set(w.maxTimestamp());
  }

  @OnTimer("windowEnd")
  public void onWindowEnd(
      OnTimerContext c,
      @StateId("count") CombiningState<Long, long[], Long> countState) {

    Long count = countState.read();
    if (count != null) {
      c.output(KV.of(c.key(), count));
    }
    countState.clear();
  }
}
