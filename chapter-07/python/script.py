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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Arrays;


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
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;


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
We set --runner=FlinkRunner to use the Flink runner. We also provided --flink_master=localhost:8081 which is the default address of a local Flink JobManager’s REST API. This tells Beam to submit the pipeline to that Flink cluster. If you had a remote cluster, you could put its host:port or a cluster identifier there. If you omit flink_master, the pipeline will start an embedded Flink instance within the Python process and run the job there. That can be convenient for quick tests (no external cluster needed), but for anything non-trivial you use a real cluster.
We included --environment_type=LOOPBACK. This is a pipeline option specific to portable runners which means “run the SDK worker in the same process (no Docker)”. We use it here for simplicity so that the Python transforms execute in the local process. Without it, Beam might attempt to use a Docker container for the Python SDK harness by default. Using LOOPBACK is fine for local testing, but note that  not recommended for production because it does isolate the execution ( mostly for ease of use).
The pipeline itself is similar to before, except instead of reading from a file, we used a beam.Create transform with some in-memory data (a few example lines). This is just to make the example self-contained; in a real scenario, you could read from files or databases. We then split the lines into words, count them, and print the counts.
Printing within a Beam pipeline is generally done via a beam.Map(print) or a beam.ParDo with a DoFn that prints. Keep in mind that on a Flink cluster, print in a distributed worker will output to that worker’s stdout (which ends up in logs). Keep in mind that on a Flink cluster, print statements in distributed workers write to each worker’s stdout, which becomes part of the task manager logs. In production, this should not be treated as a debugging convenience but as part of your observability strategy. Logging in a distributed system has real cost, performance impact, and operational consequences. Printing or logging inside distributed Beam pipelines is an observability decision, not a debugging shortcut. Uncontrolled logging can flood log storage, increase costs, slow down workers, and make real issues harder to find. Pipelines that rely on per-element logging may be logically correct but operationally undeployable.. In local execution, you see the printouts in your console.


# === Snippet ===
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
// (Other imports same as before for TextIO, Count, etc.)


# === Snippet ===
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;


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


# === Snippet ===
In the Python snippet (not executed here), we use apache_beam.io.kafka.ReadFromKafka and WriteToKafka. Each Kafka message is a key-value (KV<byte[], byte[]> in Beam’s type system for Kafka. In this case, we ignore the key and uppercase the message value. The pipeline will continuously consume from input-topic and produce to output-topic. To run this, you need a Kafka broker running at localhost:9092 and the topics created. Beam will likely need the Kafka client libraries — in Java we would add them as dependencies; in Python, since  cross-language, Beam’s expansion service will use the Java KafkaIO. This is an advanced scenario, but it demonstrates that Beam pipelines on Flink can connect to typical streaming sources in on-prem environments like Kafka.
For many beginners, a simpler approach to test streaming on Flink is to use a bounded source but run the pipeline in streaming mode (to observe differences), or use GenerateSequence as shown in Java. Keep in mind that when using Beam’s I/O connectors like Kafka on Flink, you should ensure your Beam expansion service (for cross-language) is available if using Python. The Beam Flink runner can automatically start a job server that includes expansion services for some IOs when you use --flink_master and run the pipeline (for example, it might use a combined job service container that knows about Kafka expansion).


# === Snippet ===
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
// ... other imports ...


# === Snippet ===
Running Beam Python on Spark requires using the portable runner. One convenient method provided by Beam is to use the pipeline option --output_executable_path with SparkRunner, which packages the pipeline into an executable jar. This is demonstrated in Beam’s documentation for running Python on Dataproc (Spark). We can emulate that approach:
Suppose we have the Python wordcount pipeline in a file or we use the built-in example apache_beam.examples.wordcount. We can execute:


# === Snippet ===
This will run the Beam pipeline on Spark (local in this case with 2 threads). If you have a Spark cluster, replace --master with your cluster’s master. The class that the jar contains is an entry point (org.apache.beam.runners.spark.SparkPipelineRunner as indicated in Beam docs), so we do need to specify --class because the jar’s manifest has it, courtesy of Beam’s packaging.
After it runs, check the output files in /tmp/wordcounts/ (or wherever you pointed the output). You should see the word count results files.


# === Snippet ===
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
