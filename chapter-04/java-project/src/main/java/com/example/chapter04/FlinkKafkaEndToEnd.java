package com.example.chapter04;

import java.util.Arrays;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.joda.time.Duration;

/**
 * End-to-End Streaming Pipeline: Kafka → Windowing → Aggregation → Kafka (Java).
 *
 * Mirrors the Chapter 4 combined snippet for running on Flink:
 * - FlinkRunner + streaming=true + parallelism
 * - KafkaIO read
 * - Fixed 5-minute windows + allowed lateness
 * - Parse, filter, Count.perElement
 * - Write formatted strings back to Kafka
 *
 * Requires:
 * - a Flink cluster (or local) compatible with the runner
 * - Kafka broker reachable from the runner
 * - input/output topics created
 */
public class FlinkKafkaEndToEnd {

  public static void main(String[] args) {
    String bootstrap = (args.length > 0) ? args[0] : "broker:9092";
    String inputTopic = (args.length > 1) ? args[1] : "input-topic";
    String outputTopic = (args.length > 2) ? args[2] : "output-topic";

    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.as(StreamingOptions.class).setStreaming(true);
    options.setParallelism(4);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<String, String>> kafkaInput =
        pipeline.apply(
            "ReadKafka",
            KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrap)
                .withTopic(inputTopic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata());

    PCollection<String> events = kafkaInput.apply("Values", Values.create());

    PCollection<String> windowedEvents =
        events.apply(
            "Window5Min",
            Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                .withAllowedLateness(Duration.standardMinutes(1)));

    PCollection<KV<String, Long>> counts =
        windowedEvents
            .apply(
                "Parse",
                FlatMapElements.into(TypeDescriptors.strings())
                    .via((String e) -> Arrays.asList(e.split(",")))) // example parsing
            .apply("FilterEmpty", Filter.by(e -> e != null && !e.isEmpty()))
            .apply("Count", Count.perElement());

    PCollection<String> formatted =
        counts.apply(
            "Format",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + "=" + kv.getValue()));

    formatted.apply(
        "WriteKafka",
        KafkaIO.<Void, String>write()
            .withBootstrapServers(bootstrap)
            .withTopic(outputTopic)
            .withValueSerializer(StringSerializer.class)
            .withKeySerializer(VoidSerializer.class)
            .values());

    pipeline.run();
  }
}
