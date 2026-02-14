package com.example.chapter05;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.CoGroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.CoGbkResult;
import org.apache.beam.sdk.values.KeyedPCollectionTuple;

public class CoGroupByKeyJoinExample {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    PCollection<KV<String, Integer>> ages =
        p.apply(
            "CreateAges",
            Create.of(Arrays.asList(KV.of("Alice", 25), KV.of("Bob", 30))));

    PCollection<KV<String, String>> cities =
        p.apply(
            "CreateCities",
            Create.of(Arrays.asList(KV.of("Alice", "NY"), KV.of("Charles", "LA"))));

    final TupleTag<Integer> ageTag = new TupleTag<Integer>() {};
    final TupleTag<String> cityTag = new TupleTag<String>() {};

    PCollection<KV<String, CoGbkResult>> grouped =
        KeyedPCollectionTuple.of(ageTag, ages)
            .and(cityTag, cities)
            .apply("CoGroupByKey", CoGroupByKey.create());

    grouped.apply(
        "PrettyPrint",
        ParDo.of(
            new org.apache.beam.sdk.transforms.DoFn<KV<String, CoGbkResult>, Void>() {
              @ProcessElement
              public void processElement(@Element KV<String, CoGbkResult> e) {
                String name = e.getKey();
                Iterable<Integer> agesIt = e.getValue().getAll(ageTag);
                Iterable<String> citiesIt = e.getValue().getAll(cityTag);
                System.out.println("name=" + name + " ages=" + agesIt + " cities=" + citiesIt);
              }
            }));

    p.run().waitUntilFinish();
  }
}
