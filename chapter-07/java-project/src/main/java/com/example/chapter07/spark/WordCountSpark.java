package com.example.chapter07.spark;

import java.util.Arrays;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCountSpark {
  public static void main(String[] args) {
    SparkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SparkPipelineOptions.class);

    options.setRunner(SparkRunner.class);
    options.setSparkMaster("local[4]");
    options.setAppName("BeamWordCountSpark");

    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.read().from("input.txt"))
        .apply(
            "SplitWords",
            FlatMapElements.into(TypeDescriptors.strings())
                .via(line -> Arrays.asList(line.split("\\s+"))))
        .apply("CountWords", Count.perElement())
        .apply(
            "FormatResults",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + ": " + kv.getValue()))
        .apply("WriteResults", TextIO.write().to("wordcounts-spark-out").withSuffix(".txt"));

    p.run().waitUntilFinish();
  }
}
