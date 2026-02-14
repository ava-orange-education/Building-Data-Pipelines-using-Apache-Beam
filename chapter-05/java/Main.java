// Chapter 05 - Java snippets extracted from the book manuscript
// NOTE: Snippets are separated as they appeared; you may need to adapt into a single runnable class/project.

// --- Snippet 1: Java: In Java, Beam provides a Filter transform with static methods or you can use Filter.by(...) with a predicate. For 
PCollection<String> words = p.apply(Create.of("hello", "hi", "world", "beam"));
PCollection<String> longWords = words.apply("FilterLongWords",
    Filter.by((String word) -> word.length() > 3)
);
PCollection<String> longWords = words.apply(
    Filter.by((String word) -> word.length() > 2)
);
// longWords will contain "hello", "world", "beam" (since "hi" is too short).

// --- Snippet 2
PCollection<String> words = p.apply(Create.of("hello", "hi", "world", "beam"));

// --- Snippet 3
PCollection<String> longWords = words.apply(
    "FilterLongWords",
    Filter.by((String word) -> word.length() > 3)
);

// --- Snippet 4: Java
PCollection<Integer> numbers = p.apply(Create.of(1, 2, 3, 4, 5, 6));
PCollection<Integer> evens = numbers.apply("KeepEvens",
    Filter.by((Integer num) -> num % 2 == 0)
);
// evens will contain 2, 4, 6

// --- Snippet 5: Java: In Java, the equivalent is FlatMapElements. We provide a function that returns an Iterable for each element. For e
PCollection<String> lines = p.apply(Create.of(
    "Apache Beam is unified",
    "Beam supports batch and streaming"
));
PCollection<String> words = lines.apply("SplitLines",
    FlatMapElements.into(TypeDescriptors.strings())
                   .via((String line) -> Arrays.asList(line.split(" ")))
);
// words PCollection will contain each word as a separate element.

// --- Snippet 6: Java: In Java, GroupByKey is available via the GroupByKey.create() transform. The input must be a PCollection<KV<K, V>>.
PCollection<KV<String, Integer>> sales = p.apply(Create.of(
    KV.of("apple", 3),
    KV.of("banana", 4),
    KV.of("apple", 5),
    KV.of("banana", 2),
    KV.of("orange", 7)
));
PCollection<KV<String, Iterable<Integer>>> grouped = sales.apply(GroupByKey.create());

// --- Snippet 7
// Assume these are our input collections:
PCollection<KV<String, Integer>> ages = p.apply(Create.of(
    KV.of("Alice", 25),
    KV.of("Bob", 30)
));
PCollection<KV<String, String>> cities = p.apply(Create.of(
    KV.of("Alice", "NY"),
    KV.of("Charles", "LA")
));

// --- Snippet 8
// Build the KeyedPCollectionTuple and apply CoGroupByKey
PCollection<KV<String, CoGbkResult>> grouped = KeyedPCollectionTuple
    .of(ageTag, ages)
    .and(cityTag, cities)
    .apply(CoGroupByKey.create());

// --- Snippet 9
PCollection<Integer> numbers = p.apply(Create.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
PCollectionList<Integer> partitions = numbers.apply(Partition.of(2, (PartitionFn<Integer>) (Integer x, int numPartitions) -> {
    return (x % 2 == 0) ? 0 : 1;
}));
PCollection<Integer> evens = partitions.get(0);
PCollection<Integer> odds  = partitions.get(1);

// --- Snippet 10
// For demonstration, multiply evens by 2 and odds by 3 (different processing for each partition):
PCollection<Integer> processedEvens = evens.apply(MapElements.into(TypeDescriptors.i&#8203;:contentReference[oaicite:10]{index=10}                                        .via(x -> x * 2));
PCollection<Integer> processedOdds = odds.apply(MapElements.into(TypeDescriptors.integers())
                                         .via(x -> x * 3));

// --- Snippet 11
PCollection<Integer> numbers = p.apply(Create.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

// --- Snippet 12
PCollection<Integer> evens = partitions.get(0);
PCollection<Integer> odds  = partitions.get(1);

// --- Snippet 13
// For demonstration, multiply evens by 2 and odds by 3 (different processing for each partition):
PCollection<Integer> processedEvens = evens.apply(
    MapElements.into(TypeDescriptors.integers())
               .via(x -> x * 2)
);

// --- Snippet 14
PCollection<Integer> processedOdds = odds.apply(
    MapElements.into(TypeDescriptors.integers())
               .via(x -> x * 3)
);

// --- Snippet 15: Java
PCollection<MyType> data = ...;
data.setCoder(MyTypeCoder.of());

// --- Snippet 16: Java
@DefaultCoder(MyTypeCoder.class)
public class MyType { ... }

// --- Snippet 17: Java
Pipeline pipeline = Pipeline.create();

// --- Snippet 18: Java
// 1. Input data: numbers 1 through 10
PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// --- Snippet 19
// 2. Partition into evens and odds
PCollectionList<Integer> partitions = numbers.apply("PartitionEvenOdd",
    Partition.of(2, (Integer n, int numPartitions) -> n % 2 == 0 ? 0 : 1)
);

// --- Snippet 20
// 3. Retrieve each partition
PCollection<Integer> evens = partitions.get(0);
PCollection<Integer> odds = partitions.get(1);

// --- Snippet 21
// 4. Process each partition separately
// For demonstration, compute sum of evens and sum of odds using Combine (another type of transform)
PCollection<Integer> evenSum = evens.apply("SumEvens", Combine.globally(Sum.ofIntegers()).withoutDefaults());
PCollection<Integer> oddSum = odds.apply("SumOdds", Combine.globally(Sum.ofIntegers()).withoutDefaults());

// --- Snippet 22
public class PartitionEvenOddExample {
  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();

// --- Snippet 23
    // 1. Input data: numbers 1 through 10
    PCollection<Integer> numbers =
        pipeline.apply("CreateNumbers", Create.of(1,2,3,4,5,6,7,8,9,10));

// --- Snippet 24
    // 2. Partition into evens and odds
    PCollectionList<Integer> partitions =
        numbers.apply(
            "PartitionEvenOdd",
            Partition.of(2, (Integer n, int numPartitions) -> (n % 2 == 0) ? 0 : 1)
        );

// --- Snippet 25
    // 3. Retrieve each partition
    PCollection<Integer> evens = partitions.get(0);
    PCollection<Integer> odds  = partitions.get(1);

// --- Snippet 26
    // 4. Sum each partition (single element output per branch)
    PCollection<Integer> evenSum =
        evens.apply("SumEvens", Combine.globally(Sum.ofIntegers()).withoutDefaults());
    PCollection<Integer> oddSum =
        odds.apply("SumOdds", Combine.globally(Sum.ofIntegers()).withoutDefaults());

// --- Snippet 27: In Java:
PipelineOptions options = PipelineOptionsFactory.create();
options.setRunner(DirectRunner.class);
Pipeline p = Pipeline.create(options);

// --- Snippet 28: In code, similarly:
DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
options.setRunner(DataflowRunner.class);
options.setProject("YOUR_GCP_PROJECT_ID");
options.setRegion("us-central1");
options.setTempLocation("gs://YOUR_BUCKET/temp");
options.setJobName("beam-wordcount-example");
Pipeline p = Pipeline.create(options);

