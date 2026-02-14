package com.example.chapter07.flink;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class FlinkSyntheticSource {
  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);

    options.setRunner(FlinkRunner.class);
    options.setFlinkMaster("[auto]");
    options.setParallelism(1);

    Pipeline p = Pipeline.create(options);

    PCollection<Long> numbers =
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)));

    numbers
        .apply(
            MapElements.into(TypeDescriptors.strings()).via(num -> "Number: " + num))
        .apply(
            "PrintToConsole",
            ParDo.of(
                new DoFn<String, Void>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    System.out.println(c.element());
                  }
                }));

    p.run();
  }
}
