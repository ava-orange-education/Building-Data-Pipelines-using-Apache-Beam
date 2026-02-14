package com.example.chapter07.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PubSubStreamingDataflow {
  public static void main(String[] args) {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);

    options.setRunner(org.apache.beam.runners.dataflow.DataflowRunner.class);
    options.setProject("YOUR_GCP_PROJECT_ID");
    options.setRegion("us-central1");
    options.setTempLocation("gs://YOUR_BUCKET/temp/");
    options.setStreaming(true);

    Pipeline p = Pipeline.create(options);

    String inputSub = "projects/YOUR_GCP_PROJECT_ID/subscriptions/your-input-sub";
    String outputTopic = "projects/YOUR_GCP_PROJECT_ID/topics/your-output-topic";

    PCollection<String> messages =
        p.apply("ReadPubSub", PubsubIO.readStrings().fromSubscription(inputSub));

    PCollection<String> uppercased =
        messages.apply(
            "Uppercase",
            MapElements.into(TypeDescriptors.strings()).via((String msg) -> msg.toUpperCase()));

    uppercased.apply("WritePubSub", PubsubIO.writeStrings().to(outputTopic));

    p.run(); // streaming: typically don't waitUntilFinish()
  }
}
