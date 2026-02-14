package com.beam.stateful;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.state.StateId;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.coders.VarIntCoder;

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

    current = current + c.element().getValue();
    countState.write(current);

    c.output(KV.of(c.element().getKey(), current));
  }
}