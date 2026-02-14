package com.example.chapter04;

import java.util.Arrays;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/** Reusable transform logic for both batch and streaming pipelines. */
public class WordCountTransform extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

  @Override
  public PCollection<KV<String, Long>> expand(PCollection<String> input) {
    return input
        .apply(
            "Split",
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("\\s+"))))
        .apply("FilterEmpty", Filter.by((String w) -> w != null && !w.isEmpty()))
        .apply("CountPerElement", Count.perElement());
  }
}
