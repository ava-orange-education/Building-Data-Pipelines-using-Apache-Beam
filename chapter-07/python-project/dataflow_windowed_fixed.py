import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam import window

options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=YOUR_GCP_PROJECT_ID",
    "--region=us-central1",
    "--temp_location=gs://YOUR_BUCKET/temp/",
    "--streaming",
])

input_subscription = "projects/YOUR_GCP_PROJECT_ID/subscriptions/your-input-sub"
output_topic = "projects/YOUR_GCP_PROJECT_ID/topics/your-output-topic"

with beam.Pipeline(options=options) as p:
    messages = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    decoded = messages | "Decode" >> beam.Map(lambda b: b.decode("utf-8"))

    windowed_msgs = decoded | "WindowInto1Min" >> beam.WindowInto(FixedWindows(60))

    counts = (
        windowed_msgs
        | "AddDummyKey" >> beam.Map(lambda _: ("all", 1))
        | "CountPerWindow" >> beam.CombinePerKey(sum)
    )

    formatted = counts | "Format" >> beam.Map(
        lambda kv, ts=beam.DoFn.TimestampParam: f"Window starting {ts.to_utc_datetime()}: count = {kv[1]}"
    )

    encoded = formatted | "Encode" >> beam.Map(lambda s: s.encode("utf-8"))
    encoded | "WritePubSub" >> beam.io.WriteToPubSub(topic=output_topic)
