package com.example.chapter08;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Dead-letter handling with Metrics and side outputs.
 *
 * Main output: MyRecord
 * Side output (dead-letter): raw JSON strings that failed parsing.
 */
public class ParseJsonDoFn extends DoFn<String, MyRecord> {

  /** Output tag for bad records. */
  public static final TupleTag<String> DEADLETTER_TAG = new TupleTag<String>(){};

  private final Counter malformedCounter = Metrics.counter(ParseJsonDoFn.class, "malformed_json_count");
  private transient Gson gson;

  @Setup
  public void setup() {
    gson = new Gson();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String json = c.element();
    try {
      MyRecord record = gson.fromJson(json, MyRecord.class);
      c.output(record);
    } catch (JsonSyntaxException e) {
      malformedCounter.inc();
      c.output(DEADLETTER_TAG, json);
    }
  }
}
