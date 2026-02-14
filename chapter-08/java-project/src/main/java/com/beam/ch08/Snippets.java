package com.beam.ch08;

// === Snippet ===
public class ParseJsonDoFn extends DoFn<String, MyRecord> {
    // Output tag for bad records:
    public static final TupleTag<String> DEADLETTER_TAG = new TupleTag<String>(){};
    private final Counter malformedCounter = Metrics.counter(ParseJsonDoFn.class, "malformed_json_count");


// === Snippet ===
    @Setup
    public void setup() {
        gson = new Gson();
    }


// === Snippet ===
    @ProcessElement
    public void processElement(ProcessContext c) {
        String json = c.element();
        try {
            MyRecord record = gson.fromJson(json, MyRecord.class);


// === Snippet ===
        } catch (JsonSyntaxException e) {
            // JSON was malformed
            malformedCounter.inc();
            // output the raw JSON to the dead-letter output for debugging
            c.output(DEADLETTER_TAG, json);
        }
    }
}


// === Snippet ===
public class MyDoFn extends DoFn<ElementType, OutputType> {
    private final Counter processedCount = Metrics.counter(MyDoFn.class, "processed_count");
    private final Distribution lengthDistribution = Metrics.distribution(MyDoFn.class, "element_length");


// === Snippet ===
    @ProcessElement
    public void processElement(ProcessContext c) {
        ElementType element = c.element();
        // Do some processing...


// === Snippet ===
        int length = element.toString().length();


// === Snippet ===
        // ... output results, etc.
        c.output(transform(element));
    }
}


// === Snippet ===
PipelineResult result = p.run();
result.waitUntilFinish();
MetricResults metrics = result.metrics();


// === Snippet ===
                                        .build());
for (MetricResult<Long> counter : counters.getCounters()) {
    System.out.println("Total processed: " + counter.getCommitted());
}


// === Snippet ===
DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
options.setProject("my-gcp-project");
options.setRegion("us-central1");
options.setJobName("beam-performance-example");
options.setStagingLocation("gs://my-bucket/staging");
options.setTempLocation("gs://my-bucket/temp");
options.setRunner(DataflowRunner.class);


// === Snippet ===
// Tuning:
options.setMaxNumWorkers(20);
options.setNumWorkers(5);
options.setWorkerMachineType("n2-standard-4");
options.setDiskSizeGb(50);
