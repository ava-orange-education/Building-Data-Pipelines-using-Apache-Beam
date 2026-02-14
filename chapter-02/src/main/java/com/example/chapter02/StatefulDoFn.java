package com.example.chapter02;

import java.io.Serializable;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.state.StateId;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerId;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.windowing.TimeDomain;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class StatefulDoFn extends DoFn<KV<String, Integer>, KV<String, Integer>> {

  @StateId("count")
  private final StateSpec<ValueState<Integer>> countStateSpec =
      StateSpecs.value(VarIntCoder.of());

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("count") ValueState<Integer> countState) {

    Integer current = countState.read();
    if (current == null) {
      current = 0;
    }
