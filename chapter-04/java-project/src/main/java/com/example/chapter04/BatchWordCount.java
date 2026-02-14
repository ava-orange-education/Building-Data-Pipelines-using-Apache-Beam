package com.example.chapter04;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Batch Pipeline Setup: read a bounded text file, split/filter, count, and write results.
 *
 * Mirrors the Chapter 4 snippets:
 * - ReadFromText
 * - FlatMapElements + TypeDescriptors
 * - Filter empty
 * - Count.perElement()
 * - WriteToText
 */
public class BatchWordCount {

  public static void main(String[] args) {
    String input = (args.length > 0) ? args[0] : "path/to/input.txt";
    String output = (args.length > 1) ? args[1] : "path/to/output";

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from(input));

    PCollection<String> words =
        lines.apply(
            "Split",
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("\\s+"))));

    PCollection<String> filteredWords =
        words.apply("FilterEmpty", Filter.by((String w) -> w != null && !w.isEmpty()));

    PCollection<KV<String, Long>> wordCounts =
        filteredWords.apply("CountPerElement", Count.perElement());

    PCollection<String> formatted =
        wordCounts.apply(
            "Format",
            MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> kv) -> kv.getKey() + ": " + kv.getValue()));

    formatted.apply("WriteResults", TextIO.write().to(output));

    pipeline.run().waitUntilFinish();
  }
}
