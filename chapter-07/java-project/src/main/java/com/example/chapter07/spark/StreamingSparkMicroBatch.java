package com.example.chapter07.spark;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class StreamingSparkMicroBatch {
  public static void main(String[] args) {
    SparkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SparkPipelineOptions.class);

    options.setRunner(SparkRunner.class);
    options.setSparkMaster("local[2]");
    options.setBatchIntervalMillis(2000);
    options.setAppName("BeamStreamingSpark");

    Pipeline p = Pipeline.create(options);

    PCollection<Long> numbers =
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)));

    numbers
        .apply(MapElements.into(TypeDescriptors.strings()).via(num -> "Number: " + num))
        .apply(
            "Log",
            ParDo.of(
                new DoFn<String, Void>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctx) {
                    System.out.println(ctx.element());
                  }
                }));

    p.run();
  }
}
