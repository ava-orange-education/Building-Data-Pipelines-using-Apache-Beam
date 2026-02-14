"""Chapter 4 examples (Python) — Batch vs Streaming, Windowing, Kafka IO.

Some parts (Kafka) require a reachable Kafka broker and topics.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window, trigger

def count_words(pcoll: beam.PCollection) -> beam.PCollection:
    """Reusable transform logic for both batch and streaming."""
    return (
        pcoll
        | "Split" >> beam.FlatMap(lambda line: line.split())
        | "FilterEmpty" >> beam.Filter(lambda w: w != "")
        | "CountPerElement" >> beam.combiners.Count.PerElement()
    )

def batch_read_text_and_count(input_path: str, output_prefix: str):
    with beam.Pipeline() as p:
        lines = p | "ReadText" >> beam.io.ReadFromText(input_path)
        counts = count_words(lines)
        formatted = counts | "Format" >> beam.Map(lambda kv: f"{kv[0]}: {kv[1]}")
        formatted | "WriteText" >> beam.io.WriteToText(output_prefix)

def streaming_read_kafka_window_count_write_text(bootstrap: str, topic: str, output_prefix: str):
    # DirectRunner can run in streaming mode for demos, but production streaming typically uses Flink/Spark/Dataflow.
    opts = PipelineOptions(streaming=True)
    with beam.Pipeline(options=opts) as p:
        # ReadFromKafka returns (key, value) where both are bytes by default in Python.
        from apache_beam.io.kafka import ReadFromKafka

        lines = (
            p
            | "ReadKafka" >> ReadFromKafka(
                consumer_config={"bootstrap.servers": bootstrap},
                topics=[topic],
            )
            | "ExtractValue" >> beam.Map(lambda kv: kv[1].decode("utf-8") if isinstance(kv[1], (bytes, bytearray)) else kv[1])
        )

        windowed_lines = lines | "Fixed1Min" >> beam.WindowInto(window.FixedWindows(60))

        counts = count_words(windowed_lines)
        formatted = counts | "Format" >> beam.Map(lambda kv: f"{kv[0]}: {kv[1]}")

        # For streaming-to-files, Beam typically needs windowed writes.
        formatted | "WriteWindowedText" >> beam.io.WriteToText(
            output_prefix,
            file_name_suffix=".txt",
            shard_name_template="-SS-of-NN",
        )

def streaming_window_with_triggers_demo(bootstrap: str, topic: str, output_prefix: str):
    opts = PipelineOptions(streaming=True)
    with beam.Pipeline(options=opts) as p:
        from apache_beam.io.kafka import ReadFromKafka

        events = (
            p
            | "ReadKafka" >> ReadFromKafka(
                consumer_config={"bootstrap.servers": bootstrap},
                topics=[topic],
            )
            | "ValueToStr" >> beam.Map(lambda kv: kv[1].decode("utf-8") if isinstance(kv[1], (bytes, bytearray)) else kv[1])
        )

        windowed = (
            events
            | "Window5MinWithTriggers" >> beam.WindowInto(
                window.FixedWindows(300),
                trigger=trigger.AfterWatermark(
                    early=trigger.AfterProcessingTime(60),  # early: 60s after first element in pane
                    late=trigger.AfterCount(1),             # late: whenever 1 late element arrives
                ),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=300,  # 5 minutes
            )
        )

        counts = count_words(windowed)
        formatted = counts | "Format" >> beam.Map(lambda kv: f"{kv[0]}={kv[1]}")
        formatted | "WriteWindowedText" >> beam.io.WriteToText(
            output_prefix,
            file_name_suffix=".txt",
            shard_name_template="-SS-of-NN",
        )

if __name__ == "__main__":
    # Adjust paths/brokers as needed.
    # 1) Batch demo
    # batch_read_text_and_count("path/to/input.txt", "path/to/output")

    # 2) Streaming demos (Kafka required)
    # streaming_read_kafka_window_count_write_text("localhost:9092", "text_stream", "outputs/stream_counts")
    # streaming_window_with_triggers_demo("localhost:9092", "input-topic", "outputs/triggered_counts")

    print("Chapter 4 Python examples scaffolded. Uncomment a demo in __main__ to run.")
