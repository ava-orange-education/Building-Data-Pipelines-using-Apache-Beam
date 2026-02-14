package com.example.chapter04;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

/**
 * Streaming Pipeline Input: Reading from Kafka with KafkaIO (Java).
 * Also applies fixed windowing (1 minute) and writes windowed results to text.
 *
 * Requires Kafka running and the topic to exist.
 */
public class StreamingWordCountFromKafka {

  public static void main(String[] args) {
    String bootstrap = (args.length > 0) ? args[0] : "localhost:9092";
    String topic = (args.length > 1) ? args[1] : "text_stream";
    String output = (args.length > 2) ? args[2] : "outputs/counts";

    StreamingOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingOptions.class);
    options.setStreaming(true);
    options.setJobName("streaming-wordcount");

    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<String, String>> kafkaMessages =
        pipeline.apply(
            "ReadKafka",
            KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrap)
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata());

    PCollection<String> lines = kafkaMessages.apply("Values", Values.create());

    PCollection<String> windowedLines =
        lines.apply(
            "Fixed1MinWindow",
            Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<KV<String, Long>> counts = windowedLines.apply("CountWords", new WordCountTransform());

    PCollection<String> formatted =
        counts.apply(
            "Format",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + ": " + kv.getValue()));

    formatted.apply(
        "WriteWindowedText",
        TextIO.write()
            .to(output)
            .withWindowedWrites()
            .withNumShards(1));

    pipeline.run(); // streaming: typically do not waitUntilFinish indefinitely
  }
}
