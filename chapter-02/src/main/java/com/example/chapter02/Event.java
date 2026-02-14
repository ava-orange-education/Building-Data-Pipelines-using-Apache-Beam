package com.example.chapter02;

import java.io.Serializable;

/** Simple event payload used by TimerExample and sessionization demos. */
public class Event implements Serializable {
  private final String type;
  private final String payload;

  public Event(String type, String payload) {
    this.type = type;
    this.payload = payload;
  }

  public String getType() {
    return type;
  }

  public String getPayload() {
    return payload;
  }
}
