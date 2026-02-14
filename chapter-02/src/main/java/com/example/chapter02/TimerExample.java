package com.example.chapter02;

import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.StateId;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerId;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.TimeDomain;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

/**
 * Emits a per-key count when an event-time inactivity period elapses.
 * Each new event pushes the timer forward by 10 minutes.
 */
public class TimerExample extends DoFn<KV<String, Event>, KV<String, Long>> {

  @StateId("eventCount")
  private final StateSpec<ValueState<Long>> eventCountSpec =
      StateSpecs.value(VarLongCoder.of());

  @TimerId("inactivityTimer")
  private final TimerSpec inactivityTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("eventCount") ValueState<Long> eventCountState,
      @TimerId("inactivityTimer") Timer inactivityTimer) {

    Long count = eventCountState.read();
    if (count == null) {
      count = 0L;
    }
    eventCountState.write(count + 1);

    // If no new event arrives within 10 minutes (event time) for this key, fire.
    inactivityTimer.set(c.timestamp().plus(Duration.standardMinutes(10)));
  }

  @OnTimer("inactivityTimer")
  public void onInactivityTimer(
      OnTimerContext c,
      @StateId("eventCount") ValueState<Long> eventCountState) {

    Long count = eventCountState.read();
    if (count != null && count > 0) {
      c.output(KV.of(c.key(), count));
      eventCountState.clear();
    }
  }
}
