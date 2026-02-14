package com.example;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.options.StreamingOptions;
import org.joda.time.Duration;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;

public class Main {
  public static void main(String[] args) {
    // Snippet extracted from the chapter (wrapped into a runnable main)
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(FlinkRunner.class);
    options.as(StreamingOptions.class).setStreaming(true);
    options.as(FlinkPipelineOptions.class).setParallelism(4);
    Pipeline pipeline = Pipeline.create(options);

    // Read from Kafka "input-topic"
    PCollection<KV<String, String>> kafkaInput = pipeline.apply(KafkaIO.<String, String>read()
            .withBootstrapServers("broker:9092")
            .withTopic("input-topic")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withoutMetadata());

    // Extract values (assuming key is not important)
    PCollection<String> events = kafkaInput.apply(Values.create());

    // Window into 5 minute windows, allowing 1 minute lateness
    PCollection<String> windowedEvents = events.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
            .withAllowedLateness(Duration.standardMinutes(1)));

    // Process events (e.g., count occurrences of each event type)
    PCollection<KV<String, Long>> counts = windowedEvents
            .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String e) -> Arrays.asList(e.split(",")))) // just an example parsing
            .apply(Filter.by(e -> !e.isEmpty()))
            .apply(Count.perElement());

    // Format results as strings "key=count"
    PCollection<String> formatted = counts.apply(MapElements.into(TypeDescriptors.strings())
            .via(kv -> kv.getKey() + "=" + kv.getValue()));

    // Write results to Kafka "output-topic"
    formatted.apply(KafkaIO.<Void, String>write()
            .withBootstrapServers("broker:9092")
            .withTopic("output-topic")
            .withValueSerializer(StringSerializer.class)
            .withKeySerializer(VoidSerializer.class)
            .values());

    // Execute (for cluster submission you may rely on Flink CLI instead)
    pipeline.run();
  }
}
