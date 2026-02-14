import apache_beam as beam
from apache_beam.io import ReadFromKafka, WriteToKafka

# ---- ReadFromKafka snippet (from chapter) ----
pipeline_options = beam.options.pipeline_options.PipelineOptions(streaming=True)

with beam.Pipeline(options=pipeline_options) as pipeline:
    kafka_config = {"bootstrap.servers": "localhost:9092"}

    lines = (
        pipeline
        | "ReadFromKafka" >> ReadFromKafka(
            consumer_config=kafka_config,
            topics=["text_stream"],
            key_deserializer="org.apache.kafka.common.serialization.ByteArrayDeserializer",
            value_deserializer="org.apache.kafka.common.serialization.StringDeserializer",
        )
        | "ExtractValue" >> beam.Map(lambda kv: kv[1].decode("utf-8") if isinstance(kv[1], (bytes, bytearray)) else kv[1])
    )

    # Example transform (kept minimal): uppercase each line
    result_strings = lines | "Uppercase" >> beam.Map(lambda s: s.upper())

    # ---- WriteToKafka snippet (from chapter) ----
    result_strings_kv = result_strings | "ToKV" >> beam.Map(lambda s: (None, s.encode("utf-8")))

    _ = result_strings_kv | "WriteToKafka" >> WriteToKafka(
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic="output-topic",
        key_serializer="org.apache.kafka.common.serialization.ByteArraySerializer",
        value_serializer="org.apache.kafka.common.serialization.ByteArraySerializer",
    )
