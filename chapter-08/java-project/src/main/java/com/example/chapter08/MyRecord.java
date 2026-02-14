package com.example.chapter08;

/** Minimal POJO for JSON parsing demo. */
public class MyRecord {
  public String id;
  public String payload;

  public MyRecord() {}

  public MyRecord(String id, String payload) {
    this.id = id;
    this.payload = payload;
  }

  @Override
  public String toString() {
    return "MyRecord{id='" + id + "', payload='" + payload + "'}";
  }
}
