package com.example.chapter02;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Runnable demo:
 * - Creates timestamped words
 * - Applies fixed windowing (1 minute)
 * - Counts with CombiningState + window-end timer
 *
 * Run:
 *   mvn -q -DskipTests package
 *   java -jar target/chapter-02-java-project-1.0.0-shaded.jar
 *
 * Output file:
 *   wordcount-output.txt
 */
public class WindowedWordCount {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

    Instant t0 = Instant.parse("2020-01-01T00:00:00Z");

    p.apply(
            Create.of(
                TimestampedValue.of("apple", t0.plus(Duration.standardSeconds(5))),
                TimestampedValue.of("apple", t0.plus(Duration.standardSeconds(10))),
                TimestampedValue.of("banana", t0.plus(Duration.standardSeconds(20))),
                TimestampedValue.of("apple", t0.plus(Duration.standardSeconds(80))),   // next window
                TimestampedValue.of("banana", t0.plus(Duration.standardSeconds(90)))))
        .apply(
            "ToKV(word, event)",
            MapElements.via(
                new SimpleFunction<String, KV<String, String>>() {
                  @Override
                  public KV<String, String> apply(String word) {
                    return KV.of(word, "event");
                  }
                }))
        .apply("Window1m", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply("CountStateful", ParDo.of(new WindowedWordCountStatefulDoFn()))
        .apply(
            "Format",
            MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> kv) -> kv.getKey() + " -> " + kv.getValue()))
        .apply(TextIO.write().to("wordcount-output").withSuffix(".txt").withoutSharding());

    p.run().waitUntilFinish();
  }
}
