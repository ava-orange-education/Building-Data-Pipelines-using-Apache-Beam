import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_master=localhost:8081",
])

with beam.Pipeline(options=options) as p:
    kafka_source = ReadFromKafka(
        consumer_config={
            "bootstrap.servers": "localhost:9092",
            "auto.offset.reset": "earliest",
        },
        topics=["input-topic"],
    )

    kafka_sink = WriteToKafka(
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic="output-topic",
    )

    messages = p | "ReadKafka" >> kafka_source
    processed = messages | "UppercaseValue" >> beam.Map(
        lambda kv: kv[1].decode("utf-8").upper().encode("utf-8")
    )
    processed | "WriteKafka" >> kafka_sink
