import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

flink_options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_master=localhost:8081",
    "--environment_type=LOOPBACK",
])

with beam.Pipeline(options=flink_options) as p:
    lines = p | "CreateLines" >> beam.Create([
        "apache beam runners",
        "beam pipelines on flink",
        "flink runner execution",
    ])
    words = lines | "Split" >> beam.FlatMap(lambda line: line.split())
    counts = words | "Count" >> beam.combiners.Count.PerElement()
    counts | "Print" >> beam.Map(print)
