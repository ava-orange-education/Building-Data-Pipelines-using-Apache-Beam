package com.example.chapter04;

import java.io.Serializable;
import org.joda.time.Instant;

/** Simple event model used in windowing examples. */
public class Event implements Serializable {
  private final String userId;
  private final String payload;
  private final Instant eventTime;

  public Event(String userId, String payload, Instant eventTime) {
    this.userId = userId;
    this.payload = payload;
    this.eventTime = eventTime;
  }

  public String getUserId() {
    return userId;
  }

  public String getPayload() {
    return payload;
  }

  public Instant getEventTime() {
    return eventTime;
  }

  @Override
  public String toString() {
    return "Event{userId='" + userId + "', payload='" + payload + "', eventTime=" + eventTime + "}";
  }
}
