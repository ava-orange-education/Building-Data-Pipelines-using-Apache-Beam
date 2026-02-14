package com.beam.stateful;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.coders.AvroCoder;
import org.joda.time.Duration;

public class SessionizationPipeline {

  public static class ClickEvent {}
  public static class SessionSummary {}

  public static class SessionDoFn extends DoFn<KV<String, ClickEvent>, SessionSummary> {

    @StateId("events")
    private final StateSpec<BagState<ClickEvent>> eventsSpec =
        StateSpecs.bag(AvroCoder.of(ClickEvent.class));

    @TimerId("sessionTimeout")
    private final TimerSpec sessionTimer =
        TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("events") BagState<ClickEvent> eventsState,
        @TimerId("sessionTimeout") Timer sessionTimer) {

      eventsState.add(c.element().getValue());
      sessionTimer.set(c.timestamp().plus(Duration.standardMinutes(30)));
    }

    @OnTimer("sessionTimeout")
    public void onTimeout(
        OnTimerContext c,
        @StateId("events") BagState<ClickEvent> eventsState) {

      Iterable<ClickEvent> events = eventsState.read();

      // Aggregate events into a session summary (implementation omitted)
      SessionSummary summary = new SessionSummary();

      c.output(summary);
      eventsState.clear();
    }
  }
}