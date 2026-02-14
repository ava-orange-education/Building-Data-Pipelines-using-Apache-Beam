package com.example.chapter05;

import org.apache.beam.sdk.transforms.DoFn;

/** Utility DoFn to print elements to stdout (good for DirectRunner demos). */
public class PrintFn<T> extends DoFn<T, Void> {
  private final String prefix;

  public PrintFn(String prefix) {
    this.prefix = prefix == null ? "" : prefix;
  }

  @ProcessElement
  public void processElement(@Element T element) {
    System.out.println(prefix + element);
  }
}
