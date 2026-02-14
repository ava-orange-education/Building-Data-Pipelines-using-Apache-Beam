package com.example.chapter04;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

/**
 * Trigger example mirroring the Chapter 4 snippet:
 *
 * - Fixed 5-minute windows
 * - AfterWatermark.pastEndOfWindow()
 *   - early: AfterProcessingTime + 1 minute
 *   - late: AfterPane.elementCountAtLeast(1)
 * - allowed lateness: 5 minutes
 * - accumulating fired panes
 *
 * Uses TextIO for simplicity; in real streaming use KafkaIO or another unbounded source.
 */
public class TriggersExample {

  public static void main(String[] args) {
    String input = (args.length > 0) ? args[0] : "path/to/streaming_input";
    String output = (args.length > 1) ? args[1] : "outputs/triggered_counts";

    StreamingOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingOptions.class);
    options.setStreaming(true);

    Pipeline p = Pipeline.create(options);

    PCollection<String> lines = p.apply("ReadLines", TextIO.read().from(input));

    PCollection<String> windowed =
        lines.apply(
            "WindowWithTriggers",
            Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(1)))
                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(Duration.standardMinutes(5))
                .accumulatingFiredPanes());

    PCollection<KV<String, Long>> counts =
        windowed
            .apply(
                "Split",
                FlatMapElements.into(TypeDescriptors.strings())
                    .via((String line) -> java.util.Arrays.asList(line.split("\\s+"))))
            .apply("FilterEmpty", Filter.by(w -> w != null && !w.isEmpty()))
            .apply("Count", Count.perElement());

    PCollection<String> formatted =
        counts.apply(
            "Format",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + "=" + kv.getValue()));

    formatted.apply(
        "WriteWindowedText",
        TextIO.write().to(output).withWindowedWrites().withNumShards(1));

    p.run();
  }
}
