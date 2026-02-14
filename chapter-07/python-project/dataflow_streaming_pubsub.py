import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

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

    transformed = (
        messages
        | "Decode" >> beam.Map(lambda b: b.decode("utf-8"))
        | "Uppercase" >> beam.Map(lambda s: s.upper())
        | "Encode" >> beam.Map(lambda s: s.encode("utf-8"))
    )

    transformed | "WritePubSub" >> beam.io.WriteToPubSub(topic=output_topic)
