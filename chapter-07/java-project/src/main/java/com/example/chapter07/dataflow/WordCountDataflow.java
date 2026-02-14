package com.example.chapter07.dataflow;

import java.util.Arrays;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCountDataflow {
  public static void main(String[] args) {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);

    options.setRunner(org.apache.beam.runners.dataflow.DataflowRunner.class);
    options.setProject("YOUR_GCP_PROJECT_ID");
    options.setRegion("us-central1");
    options.setTempLocation("gs://YOUR_BUCKET/temp/");
    options.setStagingLocation("gs://YOUR_BUCKET/staging/");
    options.setJobName("wordcount-example-java");

    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
        .apply(
            "SplitWords",
            FlatMapElements.into(TypeDescriptors.strings())
                .via(line -> Arrays.asList(line.split("\\s+"))))
        .apply("CountWords", Count.perElement())
        .apply(
            "FormatResults",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + ": " + kv.getValue()))
        .apply("WriteResults", TextIO.write().to("gs://YOUR_BUCKET/output/wordcounts-java").withSuffix(".txt"));

    p.run().waitUntilFinish();
  }
}
