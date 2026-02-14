import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define pipeline options for Dataflow (batch)
options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=YOUR_GCP_PROJECT_ID",
    "--region=us-central1",
    "--temp_location=gs://YOUR_BUCKET/temp/",
    "--staging_location=gs://YOUR_BUCKET/staging/",
    "--job_name=wordcount-example",
    "--save_main_session",
    # "--streaming" is not set because this is a batch pipeline
])

input_file = "gs://apache-beam-samples/shakespeare/kinglear.txt"
output_path = "gs://YOUR_BUCKET/output/wordcounts"

with beam.Pipeline(options=options) as p:
    lines = p | "ReadLines" >> beam.io.ReadFromText(input_file)
    words = lines | "Split" >> beam.FlatMap(lambda line: line.split())
    word_counts = words | "Count" >> beam.combiners.Count.PerElement()
    formatted = word_counts | "Format" >> beam.Map(lambda wc: f"{wc[0]}: {wc[1]}")
    formatted | "WriteResults" >> beam.io.WriteToText(output_path, file_name_suffix=".txt")
