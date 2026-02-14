package com.beam.ch07;

// === Snippet ===
public class WordCountDataflow {
    public static void main(String[] args) {
        // Create Dataflow pipeline options
        DataflowPipelineOptions options = PipelineOptionsFactory.create()
            .as(DataflowPipelineOptions.class);
        options.setRunner(com.google.cloud.dataflow.sdk.runners.DataflowRunner.class);
        options.setProject("YOUR_GCP_PROJECT_ID");
        options.setRegion("us-central1");
        options.setTempLocation("gs://YOUR_BUCKET/temp/");
        options.setStagingLocation("gs://YOUR_BUCKET/staging/");
        options.setJobName("wordcount-example-java");
        // options.setNumWorkers(2); // for example, to specify initial workers (optional)


// === Snippet ===
        Pipeline p = Pipeline.create(options);


// === Snippet ===
        // Read from a text source (e.g., GCS file)
        p.apply("ReadLines", TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
         // Split lines into words
         .apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings())
                                            .via(line -> Arrays.asList(line.split("\\s+"))))
         // Count each word
         .apply("CountWords", Count.perElement())
         // Format results as "word: count" strings
         .apply("FormatResults", FlatMapElements.into(TypeDescriptors.strings())
                                               .via(kv -> Arrays.asList(kv.getKey() + ": " + kv.getValue())))
         // Write results to text files in GCS
         .apply("WriteResults", TextIO.write().to("gs://YOUR_BUCKET/output/wordcounts-java")
                                       .withSuffix(".txt"));


// === Snippet ===
        p.run().waitUntilFinish();
    }
}


// === Snippet ===
DataflowPipelineOptions options = PipelineOptionsFactory.create()
    .as(DataflowPipelineOptions.class);
options.setRunner(DataflowRunner.class);
options.setProject("YOUR_GCP_PROJECT_ID");
options.setRegion("us-central1");
options.setTempLocation("gs://YOUR_BUCKET/temp/");
options.setStreaming(true);  // Enable streaming


// === Snippet ===
Pipeline p = Pipeline.create(options);


// === Snippet ===
// Define Pub/Sub I/O
String inputSub = "projects/YOUR_GCP_PROJECT_ID/subscriptions/your-input-sub";
String outputTopic = "projects/YOUR_GCP_PROJECT_ID/topics/your-output-topic";


// === Snippet ===
// Read from Pub/Sub (as Strings)
PCollection<String> messages = p.apply("ReadPubSub",
    PubsubIO.readStrings().fromSubscription(inputSub));


// === Snippet ===
// Transform each message to upper-case
PCollection<String> uppercased = messages.apply("Uppercase",
    MapElements.into(TypeDescriptors.strings())
              .via((String msg) -> msg.toUpperCase()));


// === Snippet ===
// Write to Pub/Sub topic
uppercased.apply("WritePubSub", PubsubIO.writeStrings().to(outputTopic));


// === Snippet ===
public class WordCountFlink {
    public static void main(String[] args) {
        // Create Flink pipeline options
        FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setFlinkMaster("localhost:8081");  // Flink master (JobManager) address.
        options.setParallelism(2);                // Example: set parallelism for Flink tasks.


// === Snippet ===
        Pipeline p = Pipeline.create(options);


// === Snippet ===
        p.apply("ReadLines", TextIO.read().from("input.txt"))        // adjust path as needed
         .apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings())
                                            .via(line -> Arrays.asList(line.split("\\s+"))))
         .apply("CountWords", Count.perElement())
         .apply("FormatResults", MapElements.into(TypeDescriptors.strings())
                                           .via(kv -> kv.getKey() + ": " + kv.getValue()))
         .apply("WriteResults", TextIO.write().to("wordcounts-flink-output").withSuffix(".txt"));


// === Snippet ===
        p.run().waitUntilFinish();
    }
}


// === Snippet ===
// ... (FlinkPipelineOptions setup as before)
options.setRunner(FlinkRunner.class);
// We don't need to set a special streaming flag; unbounded source triggers streaming mode.
options.setFlinkMaster("[auto]");  // use embedded or submission cluster automatically
options.setParallelism(1);        // for demo, use parallelism 1


// === Snippet ===
Pipeline p = Pipeline.create(options);


// === Snippet ===
// Generate an infinite sequence of numbers (unbounded PCollection)
PCollection<Long> numbers = p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)));


// === Snippet ===
// Map each number to a simple string and print it
numbers.apply(MapElements.into(TypeDescriptors.strings())
                          .via(num -> "Number: " + num))
       .apply("PrintToConsole", ParDo.of(new DoFn<String, Void>() {
           @ProcessElement
           public void processElement(ProcessContext c) {
               System.out.println(c.element());
           }
       }));


// === Snippet ===
public class WordCountSpark {
    public static void main(String[] args) {
        SparkPipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
        options.setRunner(SparkRunner.class);
        options.setSparkMaster("local[4]");  // using local Spark with 4 threads
        // options.setSparkMaster("spark://spark-master:7077"); // example for cluster master URL
        options.setAppName("BeamWordCountSpark");


// === Snippet ===
        Pipeline p = Pipeline.create(options);


// === Snippet ===
        p.apply("ReadLines", TextIO.read().from("input.txt"))
         .apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings())
                                            .via(line -> Arrays.asList(line.split("\\s+"))))
         .apply("CountWords", Count.perElement())
         .apply("FormatResults", MapElements.into(TypeDescriptors.strings())
                                           .via(kv -> kv.getKey() + ": " + kv.getValue()))
         .apply("WriteResults", TextIO.write().to("wordcounts-spark-out").withSuffix(".txt"));


// === Snippet ===
        p.run().waitUntilFinish();
    }
}


// === Snippet ===
SparkPipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
options.setRunner(SparkRunner.class);
options.setSparkMaster("local[2]");
options.setBatchIntervalMillis(2000);  // 2-second micro-batch interval
options.setAppName("BeamStreamingSpark");


// === Snippet ===
Pipeline p = Pipeline.create(options);


// === Snippet ===
// Simulate streaming data by generating numbers 0,1,2... every second
PCollection<Long> numbers = p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)));


// === Snippet ===
// Map to strings and log them (using a DoFn to avoid collecting huge output)
numbers.apply(MapElements.into(TypeDescriptors.strings()).via(num -> "Number: " + num))
       .apply("Log", ParDo.of(new DoFn<String, Void>() {
           @ProcessElement
           public void processElement(ProcessContext ctx) {
               System.out.println(ctx.element());
           }
       }));
