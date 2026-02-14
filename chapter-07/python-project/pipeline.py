# === Snippet ===
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# === Snippet ===
with beam.Pipeline(options=options) as p:
    # Read a text file from GCS
    lines = p | "ReadLines" >> beam.io.ReadFromText(input_file)
    # Split lines into words
    words = lines | "Split" >> beam.FlatMap(lambda line: line.split())
    # Count each word occurrence
    word_counts = words | "Count" >> beam.combiners.Count.PerElement()
    # Format and write the results to GCS (as newline-delimited text)
    formatted = word_counts | "Format" >> beam.Map(lambda wc: f"{wc[0]}: {wc[1]}")
    formatted | "WriteResults" >> beam.io.WriteToText(output_path, file_name_suffix=".txt")


# === Snippet ===
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


# === Snippet ===
with beam.Pipeline(options=options) as p:
    # Read messages (bytes) from Pub/Sub subscription
    messages = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    # Transform the messages (for example, decode and uppercase the text)
    # Assume messages are text encoded in UTF-8
    transformed = messages | "Decode" >> beam.Map(lambda b: b.decode('utf-8')) \
                            | "Uppercase" >> beam.Map(lambda s: s.upper())
    # Write transformed messages to an output Pub/Sub topic
    transformed | "WritePubSub" >> beam.io.WriteToPubSub(topic=output_topic)


# === Snippet ===
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


# === Snippet ===
with beam.Pipeline(options=options) as p:
    messages = p | "ReadPubSub" >> beam.io.ReadFromPubSub(
        subscription=input_subscription
    )


# === Snippet ===
    transformed = (
        messages
        | "Decode" >> beam.Map(lambda b: b.decode("utf-8"))
        | "Uppercase" >> beam.Map(lambda s: s.upper())
        | "Encode" >> beam.Map(lambda s: s.encode("utf-8"))  # Explicitly convert back to bytes
    )


# === Snippet ===
    transformed | "WritePubSub" >> beam.io.WriteToPubSub(topic=output_topic)


# === Snippet ===
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam import window


# === Snippet ===
with beam.Pipeline(options=options) as p:
    messages = p | "ReadPubSub" >> beam.io.ReadFromPubSub(
        subscription=input_subscription
    )


# === Snippet ===
    # Decode bytes → string
    decoded = messages | "Decode" >> beam.Map(lambda b: b.decode("utf-8"))


# === Snippet ===
    # Apply 1-minute fixed windows
    windowed = decoded | "WindowInto1Min" >> beam.WindowInto(
        FixedWindows(60)
    )


# === Snippet ===
    # Count number of messages per window
    counts = (
        windowed
        | "AddDummyKey" >> beam.Map(lambda x: ("all", 1))
        | "CountPerWindow" >> beam.CombinePerKey(sum)
    )


# === Snippet ===
    # Format output: "window_start → count"
    formatted = counts | "Format" >> beam.Map(
        lambda kv, ts=beam.DoFn.TimestampParam:
            f"Window starting {ts.to_utc_datetime()}: count = {kv[1]}"
    )


# === Snippet ===
    # Encode back to bytes for Pub/Sub
    encoded = formatted | "Encode" >> beam.Map(lambda s: s.encode("utf-8"))


# === Snippet ===
    encoded | "WritePubSub" >> beam.io.WriteToPubSub(topic=output_topic)




# === Snippet ===
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# === Snippet ===
with beam.Pipeline(options=flink_options) as p:
    # We'll do a simple word count on a small in-memory data for demonstration
    lines = p | "CreateLines" >> beam.Create([
        "apache beam runners",
        "beam pipelines on flink",
        "flink runner execution"
    ])
    words = lines | "Split" >> beam.FlatMap(lambda line: line.split())
    counts = words | "Count" >> beam.combiners.Count.PerElement()
    # Print results (in real Flink, prints go to worker logs; for local run, it will output here)
    counts | "Print" >> beam.Map(print)



# === Snippet ===
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka


# === Snippet ===
with beam.Pipeline(options=options) as p:
    kafka_source = ReadFromKafka(
        consumer_config={'bootstrap.servers': 'localhost:9092',
                         'auto.offset.reset': 'earliest'},
        topics=['input-topic']
    )
    kafka_sink = WriteToKafka(
        producer_config={'bootstrap.servers': 'localhost:9092'},
        topic='output-topic'
    )
    # Read from Kafka topic
    messages = p | "ReadKafka" >> kafka_source
    # Process messages (for example, just convert value to uppercase assuming UTF-8 bytes)
    processed = messages | "UppercaseValue" >> beam.Map(lambda kv: kv[1].decode('utf-8').upper().encode('utf-8'))
    # Write to output Kafka topic
    processed | "WriteKafka" >> kafka_sink


