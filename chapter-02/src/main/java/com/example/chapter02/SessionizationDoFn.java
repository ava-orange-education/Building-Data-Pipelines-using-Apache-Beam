package com.example.chapter02;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateId;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerId;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.TimeDomain;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Sessionization pattern using BagState and an event-time "timeout" timer.
 *
 * Behavior:
 * - Each event is stored in BagState for its key.
 * - Each event pushes a timer out by SESSION_GAP.
 * - When the timer fires, we emit a SessionSummary and clear state.
 *
 * Note: This is a simplified pattern; production pipelines often include late-data handling,
 * watermark/trigger configuration, and bounded state growth safeguards.
 */
public class SessionizationDoFn extends DoFn<KV<String, ClickEvent>, SessionSummary> {

  private static final Duration SESSION_GAP = Duration.standardMinutes(30);

  @StateId("events")
  private final StateSpec<BagState<ClickEvent>> eventsSpec =
      StateSpecs.bag(SerializableCoder.of(ClickEvent.class));

  @TimerId("sessionTimeout")
  private final TimerSpec sessionTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("events") BagState<ClickEvent> eventsState,
      @TimerId("sessionTimeout") Timer sessionTimeout) {

    ClickEvent ev = c.element().getValue();
    // If your ClickEvent carries its own event time, you'd typically also set the element timestamp
    // upstream (TimestampedValue) so c.timestamp() matches that event time.
    eventsState.add(ev);

    // Extend the session: if no more events arrive within SESSION_GAP, the timer fires.
    sessionTimeout.set(c.timestamp().plus(SESSION_GAP));
  }

  @OnTimer("sessionTimeout")
  public void onTimeout(
      OnTimerContext c,
      @StateId("events") BagState<ClickEvent> eventsState) {

    List<ClickEvent> events = new ArrayList<>();
    for (ClickEvent e : eventsState.read()) {
      events.add(e);
    }

    if (!events.isEmpty()) {
      SessionSummary summary = SessionSummary.from(c.key(), events);
      c.output(summary);
    }

    eventsState.clear();
  }

  /** Example click event payload. */
  public static class ClickEvent implements Serializable {
    private final String url;
    private final Instant eventTime;

    public ClickEvent(String url, Instant eventTime) {
      this.url = url;
      this.eventTime = eventTime;
    }

    public String getUrl() {
      return url;
    }

    public Instant getEventTime() {
      return eventTime;
    }
  }

  /** Output summary for a session. */
  public static class SessionSummary implements Serializable {
    private final String userId;
    private final long eventCount;
    private final Instant sessionStart;
    private final Instant sessionEnd;
    private final List<String> urls;

    public SessionSummary(
        String userId, long eventCount, Instant sessionStart, Instant sessionEnd, List<String> urls) {
      this.userId = userId;
      this.eventCount = eventCount;
      this.sessionStart = sessionStart;
      this.sessionEnd = sessionEnd;
      this.urls = urls;
    }

    public String getUserId() {
      return userId;
    }

    public long getEventCount() {
      return eventCount;
    }

    public Instant getSessionStart() {
      return sessionStart;
    }

    public Instant getSessionEnd() {
      return sessionEnd;
    }

    public List<String> getUrls() {
      return urls;
    }

    public static SessionSummary from(String userId, List<ClickEvent> events) {
      Instant min = events.get(0).getEventTime();
      Instant max = events.get(0).getEventTime();
      List<String> urls = new ArrayList<>();

      for (ClickEvent e : events) {
        urls.add(e.getUrl());
        if (e.getEventTime().isBefore(min)) {
          min = e.getEventTime();
        }
        if (e.getEventTime().isAfter(max)) {
          max = e.getEventTime();
        }
      }

      return new SessionSummary(userId, events.size(), min, max, urls);
    }

    @Override
    public String toString() {
      return "SessionSummary{userId="
          + userId
          + ", eventCount="
          + eventCount
          + ", sessionStart="
          + sessionStart
          + ", sessionEnd="
          + sessionEnd
          + ", urls="
          + urls
          + "}";
    }
  }
}
