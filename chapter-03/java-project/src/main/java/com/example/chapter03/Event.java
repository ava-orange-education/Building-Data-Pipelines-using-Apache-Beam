package com.example.chapter03;

import java.io.Serializable;

/**
 * Minimal event model for the Chapter 3 examples.
 * timestampMillis is epoch milliseconds (event-time).
 */
public class Event implements Serializable {
  private String userId;
  private String type;
  private long timestampMillis;
  private long value;

  public Event() {}

  public Event(String userId, String type, long timestampMillis, long value) {
    this.userId = userId;
    this.type = type;
    this.timestampMillis = timestampMillis;
    this.value = value;
  }

  public String getUserId() { return userId; }
  public String getType() { return type; }
  public long getTimestampMillis() { return timestampMillis; }
  public long getValue() { return value; }

  @Override
  public String toString() {
    return "Event{userId='" + userId + "', type='" + type + "', timestampMillis=" + timestampMillis + ", value=" + value + "}";
  }
}
