# === Snippet (assumed commands) ===
--runner=DataflowRunner: Specifies that the Dataflow service will execute the pipeline.
--project: Your GCP project ID where the Dataflow job will run.
--region: The region for the Dataflow job (for example, us-central1).  best to choose a region close to your data sources.
--temp_location: A GCS path for temporary files (e.g., gs://<your-bucket>/temp). This is required; Dataflow uses it for staging data and as a default for other storage needs.
--staging_location: (Java-specific) A GCS path to stage the pipeline binary (if not set, a default in temp_location is used).


# === Snippet (unclear) ===
--output: (not a built-in option, but in examples we often pass an output path parameter for where results should go; this is pipeline-specific).
--streaming: A flag to indicate streaming mode. Set this to true for unbounded streaming pipelines. In batch pipelines (bounded data), this is not needed (defaults to false).
Additional settings  --autoscalingAlgorithm, --numWorkers, --maxWorkers can control worker scaling, but those have reasonable defaults. You can usually omit them for a simple deployment.
In Python, these options can be passed in a list to PipelineOptions, or you can use argparse to parse command-line flags. In Java, you typically use the PipelineOptionsFactory to create a DataflowPipelineOptions object. We demonstrate both. However in production, pipeline options should always be parsed from command-line arguments and treated as deployment-time configuration. Hardcoding values is acceptable only for small demonstrations. For reproducible and automated deployments, the same pipeline binary or script should be deployable to different environments simply by changing the runtime parameters.


# === Snippet (unclear) ===
# Define pipeline options for Dataflow
options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=YOUR_GCP_PROJECT_ID",
    "--region=us-central1",               # or your desired Dataflow region
    "--temp_location=gs://YOUR_BUCKET/temp/",
    "--staging_location=gs://YOUR_BUCKET/staging/",
    "--job_name=wordcount-example",       # Dataflow job name (lowercase required)
    "--save_main_session",                # for serialization of session (if needed)
    # "--streaming" is not set because this is a batch pipeline
])


# === Snippet (unclear) ===
# Example pipeline: WordCount
input_file = "gs://apache-beam-samples/shakespeare/kinglear.txt"  # sample public data
output_path = "gs://YOUR_BUCKET/output/wordcounts"


# === Snippet (unclear) ===
In this Python code, we construct a PipelineOptions with the Dataflow runner and necessary settings (project, region, ). The pipeline itself is the classic word count: read from a text source, do transformations, and write to an output. The input in this example is a public sample file on GCS, and the output is written to your GCS bucket. After writing this script (e.g., as wordcount_dataflow.py), you would run it from your terminal. For example:


# === Snippet ===
python wordcount_dataflow.py


# === Snippet (unclear) ===
Tip: In the options, --save_main_session is used (without a value, which implies True). This option saves the state of the main Python session, which is useful if you define any functions or classes in your __main__ module that need to be used on the workers. It ensures they get pickled and sent to the Dataflow worker machines. This is often needed for pipelines defined in interactive environments or notebooks.


# === Snippet (unclear) ===
We use DataflowPipelineOptions to specify runner and related configurations. We set the project ID, region, and Cloud Storage locations for temp and staging. We also set a job name (must be lowercase and unique for each run).
The pipeline is constructed using the Beam Java SDK transforms: TextIO.read() to read an input text file from GCS, a FlatMapElements to split lines into words (using a simple whitespace split), Count.perElement() to count occurrences of each unique word, and TextIO.write() to write the output to GCS.
We then run the pipeline. The waitUntilFinish() call will cause the program to block until the Dataflow job completes (which might be some time, depending on data size). In many cases, you might omit waitUntilFinish() to allow the Java process to exit after submission, unless you need to ensure completion or get result state.
To run this Java pipeline, you would compile it and then execute it. If using Maven, ensure you have the Dataflow runner dependency in your pom.xml (e.g., beam-runners-google-cloud-dataflow-java as runtime scope). You can run it using Maven exec or package it as a “fat jar”. For example, one approach is:


# === Snippet (unclear) ===
Add Dataflow dependencies: In pom.xml, include:
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-google-cloud-dataflow-java</artifactId>
  <version>2.XX.0</version>  <!-- use the Beam version you are using -->
  <scope>runtime</scope>
</dependency>


# === Snippet (unclear) ===
Compile and run: Use mvn compile exec:java -Dexec.mainClass=your.package.WordCountDataflow -Dexec.args="--project=YOUR_GCP_PROJECT_ID --region=us-central1 --tempLocation=gs://YOUR_BUCKET/temp/ --stagingLocation=gs://YOUR_BUCKET/staging/". Maven will pass those args into PipelineOptionsFactory.fromArgs(args). In our code, we used PipelineOptionsFactory.create() and set values programmatically for clarity. Alternatively, you could use fromArgs(args) to parse command-line arguments directly into the options.
Alternatively, package a self-executable JAR. Beam’s documentation suggests using the Maven Shade plugin to create a fat jar (bundling all dependencies) After packaging, you could run: java -jar target/your-jar.jar --runner=DataflowRunner --project=... [other options] to submit to Dataflow. This might be useful if you need to schedule the job or run it outside your development environment (e.g., from a CI/CD pipeline).


# === Snippet (unclear) ===
You must specify --streaming=true in your pipeline options (otherwise Dataflow will treat it as a batch job and likely error out if you use unbounded sources)


# === Snippet (unclear) ===
# Pipeline options for Dataflow streaming
options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=YOUR_GCP_PROJECT_ID",
    "--region=us-central1",
    "--temp_location=gs://YOUR_BUCKET/temp/",
    "--streaming"  # Enable streaming mode
])


# === Snippet (unclear) ===
# Pub/Sub topic and subscription (ensure these exist in your project)
input_subscription = "projects/YOUR_GCP_PROJECT_ID/subscriptions/your-input-sub"
output_topic = "projects/YOUR_GCP_PROJECT_ID/topics/your-output-topic"


# === Snippet (unclear) ===
# Pipeline options for Dataflow streaming
options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=YOUR_GCP_PROJECT_ID",
    "--region=us-central1",
    "--temp_location=gs://YOUR_BUCKET/temp/",
    "--streaming"
])


# === Snippet (unclear) ===
# Pub/Sub topic and subscription
input_subscription = "projects/YOUR_GCP_PROJECT_ID/subscriptions/your-input-sub"
output_topic = "projects/YOUR_GCP_PROJECT_ID/topics/your-output-topic"


# === Snippet (unclear) ===
options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=YOUR_GCP_PROJECT_ID",
    "--region=us-central1",
    "--temp_location=gs://YOUR_BUCKET/temp/",
    "--streaming"
])


# === Snippet (unclear) ===
input_subscription = "projects/YOUR_GCP_PROJECT_ID/subscriptions/your-input-sub"
output_topic = "projects/YOUR_GCP_PROJECT_ID/topics/your-output-topic"


# === Snippet (unclear) ===
p.run();  // For streaming, typically do not waitUntilFinish


# === Snippet (unclear) ===
In the Java streaming example, we use PubsubIO which is a built-in I/O connector in the Beam Java SDK for Google Cloud Pub/Sub. The pipeline reads strings from a subscription, transforms them, and writes strings to a topic. We set options.setStreaming(true) to ensure the Dataflow runner knows this is a streaming job. We call p.run() without waitUntilFinish() because a streaming job will never complete on its own – you would stop it manually when needed (or use Dataflow’s drain feature to finish processing buffered data and then stop).


# === Snippet ===
mvn compile exec:java \
  -Dexec.mainClass=your.package.StreamingPipelineDataflow \
  -Dexec.args="--project=YOUR_GCP_PROJECT_ID --region=us-central1 \
               --tempLocation=gs://YOUR_BUCKET/temp/ --streaming=true"


# === Snippet (unclear) ===
Or include options.setStreaming(true) as we did in code. Make sure the Pub/Sub topic and subscription exist and your GCP credentials have access to them.


# === Snippet (unclear) ===
Stopping a Dataflow job: If you want to stop a streaming job, you can cancel it from the UI or use the command gcloud dataflow jobs cancel <job-id>. There is also a drain option which tries to finish processing all pending data and then stop, which is useful for graceful shutdown of streaming pipelines (drain can be initiated via gcloud dataflow jobs drain <job-id> or in the UI).


# === Snippet (unclear) ===
Start a local cluster by running bin/start-cluster.sh which launches a Flink JobManager and TaskManager on your machine. By default, the JobManager’s REST interface will be at http://localhost:8081.


# === Snippet (unclear) ===
Add Python: The Beam Python SDK supports Flink via the portable Flink runner. Typically, when you install apache-beam, it includes the necessary bits to run with Flink. You may need a Flink job server (which can run via a Docker container) for complex setups, but Beam can also start an embedded Flink for you. We demonstrate a simple approach using the FlinkRunner option, which in newer Beam versions automatically uses the portable runner behind the scenes.


# === Snippet (unclear) ===
Flink Runner flavors (classic portable):  important to know that Beam’s Flink runner comes in two flavors: the classic runner (for Java only) and the portable runner (for multiple languages including Java, Python, Go). If you are writing your pipeline in Java and only need Java, you can use the classic Flink runner which integrates more directly with Flink’s APIs. If you are using Python (or want cross-language), Beam will use the portable Flink runner, which involves a job service translating the pipeline to Flink and Python SDK harnesses for user code. For the purpose of this beginner chapter, we  dive deep into the architecture differences; just keep in mind that Python on Flink will spin up some additional services under the hood (which Beam handles for you when you use --runner=FlinkRunner in Python).


# === Snippet (unclear) ===
# PipelineOptions for Flink
flink_options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_master=localhost:8081",  # Flink Master URL; assuming local cluster.
    # If flink_master is not specified, Beam will start an embedded Flink cluster.
    "--environment_type=LOOPBACK"     # (Optional) run Python code on the same process for testing.
])


# === Snippet ===
python wordcount_flink.py


# === Snippet (unclear) ===
For a more realistic example, you could have ReadFromText("input.txt") and WriteToText("output.txt") in the pipeline. If you do that on Flink, make sure the path is accessible by the Flink worker machines. In local mode, local file paths are fine; in a cluster, using a distributed filesystem (HDFS) or mounting shared storage might be needed.


# === Snippet (unclear) ===
We use FlinkPipelineOptions (available when you include the Beam Flink runner dependency) to configure the pipeline. We set the runner to FlinkRunner.class.options.setFlinkMaster("localhost:8081") points to the Flink cluster’s master. If we wanted to run this in embedded mode, we could set it to “[auto]” or leave it unset (in which case Beam treats it as embedded) “[auto]” is a special value that the Flink runner uses to denote an embedded Flink deployment. If you leave it null, it also defaults to embedded.
We explicitly set setParallelism(2) to show that you can control the degree of parallelism for the Flink execution (this corresponds to Flink’s parallelism for tasks). If not set, Flink’s default will be used (which might be 1 for embedded or the cluster default for remote).
The pipeline logic is identical to earlier WordCount examples. We read from “input.txt” (you would replace this with a real path, e.g., a local file path or an HDFS path if running on a cluster), and write to wordcounts-flink-output-*.txt files.
After p.run(), we call waitUntilFinish() to block until done (since this is a batch job, it will terminate).


# === Snippet (unclear) ===
Run it as a regular Java program (through your IDE or java -cp ... WordCountFlink). This will execute the main, which in turn submits to Flink. If  a remote Flink cluster, your program will act as a client that sends the job to the cluster and waits for it to finish.
Package it as a JAR and use Flink’s CLI. For instance, build a jar named wordcount-beam-flink.jar. Then use:
$FLINK_HOME/bin/flink run -c your.package.WordCountFlink wordcount-beam-flink.jar
This submits the job to a Flink cluster (it uses Flink’s REST or RPC mechanisms). Note that if you do this, the pipeline’s FlinkMaster should usually be set to [auto] (so it uses the cluster you submit to). If you leave FlinkMaster as “localhost:8081” and run on a different host, it might try to connect to a local Flink that is correct. However, using flink run inherently sends the job to the cluster you target, so “[auto]” is appropriate.


# === Snippet (unclear) ===
Flink runner does require a special --streaming flag; it will automatically run in streaming mode if the pipeline has unbounded sources. (Beam’s StreamingOptions flag is mainly for Dataflow; on Flink, a pipeline with an unbounded source is naturally streaming.)


# === Snippet (unclear) ===
p.run();  // For streaming, don't waitUntilFinish in this demo


# === Snippet (unclear) ===
In this Java streaming example, we use GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)) to produce a new Long every second indefinitely. GenerateSequence is a handy Beam transform to create an unbounded stream of incremental numbers. This is useful for testing or simulating streaming data. We then map each number to a string and print it. We use a DoFn to print because  a side-effect (alternatively, one could use a logging framework).


# === Snippet (unclear) ===
options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_master=localhost:8081"
    # No need for streaming flag explicitly
])


# === Snippet (unclear) ===
Parallelism and resources: Flink allows you to set a parallelism for the job or individual operators. We showed setting a pipeline-wide parallelism. The Flink runner will map Beam ParDo/GroupByKey  to Flink operators. Ensure your Flink cluster has enough Task Slots to accommodate the parallelism. For example, if you have a local cluster with 4 slots, and you set parallelism 8, the job will be queued or will not run fully due to insufficient slots.
State and Checkpointing: If your Beam pipeline uses stateful transforms or timers (which are advanced features), Flink’s exactly-once state model will ensure consistency. You should configure a checkpoint interval in Flink for streaming jobs (this can be done via Flink config or programmatically by the Flink runner’s options, e.g., options.setCheckpointingInterval(seconds) if exposed). By default, the Flink runner may use a default interval (perhaps 5s or 10s). The Flink runner exposes a checkpointing interval option in milliseconds (for example, options.setCheckpointingInterval(60000) for 60 seconds). If this option is not set, checkpointing is considered unset and relies on Flink defaults or may be effectively disabled. There is no guaranteed built-in default such as “5–10 seconds” at the Beam layer. For production, writing checkpoints to durable storage ( a distributed filesystem or database) is crucial. You might configure Flink’s state backend (e.g., RocksDB state backend with checkpoint directory on HDFS) for scale streaming.


# === Snippet (unclear) ===
FlinkRunner  PortableRunner usage: In newer Beam versions, an alternative way to run on Flink is using the PortableRunner with a --job_endpoint that points to a Flink JobServer. For example, one could start the Beam Flink Job Server Docker container and then use --runner=PortableRunner --job_endpoint=localhost:8099. This is an advanced deployment approach. For beginners, using FlinkRunner directly as we shown is simpler. Just be aware that under the hood, FlinkRunner (for Python) is actually launching that job server for you.


# === Snippet (unclear) ===
A local Spark setup (Spark can run locally for testing using a local master URL like local[4] for 4 threads).


# === Snippet (unclear) ===
If using Java, include the Beam Spark runner dependency. For Spark 3, the artifact is beam-runners-spark-3. Ensure the version matches your Beam SDK. For example:


# === Snippet (unclear) ===
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-spark-3</artifactId>
  <version>2.XX.0</version>
</dependency>


# === Snippet (unclear) ===
Embedded in a Spark driver (local or cluster): You run your Beam pipeline as a Spark job, typically by submitting via spark-submit. The Beam runner will create the necessary Spark RDDs or DStreams under the hood. If you run the pipeline in local mode (e.g., via a main method and Spark local), it uses the Spark libraries on your machine.
Using a Job Server (portable): You start a Beam Spark job server that communicates with a Spark cluster, then run the pipeline with PortableRunner pointing to that server.


# === Snippet (unclear) ===
We use SparkPipelineOptions (from the Spark runner library) to set up the pipeline. We specify SparkRunner as the runner.
We set SparkMaster to local[4] which means the job will run using 4 threads on the local machine (no external Spark cluster needed). If you wanted to run on a Spark cluster, you put the Spark master URL here, e.g., spark://host:port for a standalone cluster or yarn for YARN mode.


# === Snippet (unclear) ===
Running this is as simple as running the main() (through your IDE or java -jar ...). Because we specified local[4], it will start a local Spark context within the process and execute. If we specified a cluster master, the Spark runner would try to connect to that cluster and run the job there. In cluster mode, typically you would submit via spark-submit rather than running directly.


# === Snippet (unclear) ===
Package the application into a JAR (including Beam and runner dependencies). Use the Maven Shade plugin to create a fat jar if you plan to use spark-submit. The Beam Spark runner, your pipeline, and any connectors should be in that jar.
Use spark-submit to submit the job. For example:


# === Snippet ===
spark-submit --master spark://spark-master:7077 \
    --class your.package.WordCountSpark \
    /path/to/wordcount-spark-bundled.jar


# === Snippet (unclear) ===
You would not include --runner=SparkRunner in spark-submit; that is specified in the code. The Spark master is provided via the --master flag (which will override what you put in options.setSparkMaster if it conflicts). In practice, you might leave setSparkMaster out of code and rely on --master at submission time. The SparkPipelineOptions also has a getSparkMaster() property which will default to local if not set.


# === Snippet ===
# Command-line invocation to create an executable jar for the Python pipeline
python -m apache_beam.examples.wordcount \
    --runner=SparkRunner \
    --output_executable_path=wordcount_spark_job.jar \
    --spark_version=3 \
    --output=/tmp/wordcounts/result


# === Snippet (assumed commands) ===
--runner=SparkRunner: Tells Beam to use the Spark portable runner in "batch job bundling" mode (since we provided output_executable_path).
--output_executable_path=wordcount_spark_job.jar: This tells Beam to bundle the entire pipeline (with a small harness) into the specified jar file. After running this command, you will get wordcount_spark_job.jar.
--spark_version=3: Ensures the jar is built targeting Spark 3.x (since Beam supports Spark 3; Spark 2 is deprecated).
--output=/tmp/wordcounts/result: This is a pipeline-specific option for the WordCount example, indicating the output prefix for results.  unrelated to runner config.


# === Snippet ===
spark-submit --master local[2] wordcount_spark_job.jar


# === Snippet (unclear) ===
Streaming with the Spark runner uses Spark’s DStream micro-batching under the hood. Essentially, Beam will create a Spark Streaming context with a batch interval (by default 1 second, configurable via --batchIntervalMillis). The pipeline transforms are applied each micro-batch of data.


# === Snippet (unclear) ===
p.run();  // don't wait, streaming job


# === Snippet (unclear) ===
We set batchIntervalMillis to 2000 ms (2 seconds). This means the Spark runner will configure Spark Streaming to batch events in 2-second windows. The GenerateSequence is generating one element per second, so effectively each micro-batch will have 2 elements.
We use GenerateSequence to simulate an unbounded source. This triggers the pipeline to be treated as streaming.


# === Snippet (unclear) ===
Compatibility: Use Beam versions that support your Spark version. As of Beam 2.48.0+, Spark 3.2+ is supportedSpark 2 is deprecated. Make sure the beam-runners-spark-3 library is the correct one.
Cluster integration: If using YARN, you can set --sparkMaster=yarn in SparkPipelineOptions or simply rely on spark-submit --master yarn. On Dataproc (Google’s managed Hadoop/Spark service), you can use Beam on Spark as well (there are guides to run Beam jobs on Dataproc’s Spark).


# === Snippet (unclear) ===
Dependencies: Similar to Flink, if you use any connectors ( JDBC, Hadoop InputFormat, ), include them in your job’s classpath. Spark’s --jars option can attach additional jars if you do bundle them. For example, if reading from a JDBC in Beam, you need the JDBC driver jar available to the Spark executors.


# === Snippet (unclear) ===
Flink: Uses Flink’s checkpointing mechanism. Flink can achieve exactly-once processing of stateful operations and sources that support replay (Kafka, for example) by storing checkpoints and, on failure, restoring state and replaying from the last checkpoint. With Beam on Flink, if your pipeline fails, you can restart from the last checkpoint/savepoint to resume without data loss. You should configure checkpointingInterval and state backend for production.


# === Snippet (unclear) ===
Using --requirements_file to have Dataflow install pip packages on workers.
Using --setup_file to package your code and dependencies.
Using container images (Beam Python allows custom container via --worker_harness_container_image).
Ensure you test that your dependencies are correctly picked up on Dataflow. Nothing is worse than a job that fails after a few minutes because a module was found on the worker.


# === Snippet (unclear) ===
Use Reliable Storage for State/Checkpoints: Configure Flink to store checkpoints and savepoints in a distributed filesystem (HDFS, NFS, S3, ) accessible by all nodes. For Spark Streaming, if you use any stateful operations (like Beam’s stateful DoFn or simply Spark’s updateStateByKey indirectly through Beam state API), set a checkpoint directory to a durable location as well. This could be an HDFS path via SparkPipelineOptions.setCheckpointDir(). Reliable storage ensures that if the driver goes down or cluster restarts, you can resume the stream.


# === Snippet (unclear) ===
On Spark, parallelism is determined by default by the number of partitions of RDDs/Dataframes. Beam likely decides an appropriate partitioning (for example, after a GroupByKey, the number of partitions might equal a default parallelism or number of keys). You can influence it by setting --parallelism in Spark runner or by using .withNumShards() on some Beam transforms ( Write) to control output parallelism.
