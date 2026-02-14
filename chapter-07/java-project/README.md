# Chapter 07 - Java Project

Packages:
- `com.example.chapter07.dataflow` (Dataflow batch + streaming Pub/Sub)
- `com.example.chapter07.flink` (Flink synthetic unbounded source)
- `com.example.chapter07.spark` (Spark batch + streaming micro-batch)

Run examples with exec-maven-plugin, e.g.:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.chapter07.spark.WordCountSpark
```

For Dataflow examples, replace placeholders (`YOUR_GCP_PROJECT_ID`, `YOUR_BUCKET`) first.
