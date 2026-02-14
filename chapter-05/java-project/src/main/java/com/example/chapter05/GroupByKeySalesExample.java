package com.example.chapter05;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GroupByKeySalesExample {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    PCollection<KV<String, Integer>> sales =
        p.apply(
            "CreateSales",
            Create.of(
                Arrays.asList(
                    KV.of("apple", 3),
                    KV.of("banana", 4),
                    KV.of("apple", 5),
                    KV.of("banana", 2),
                    KV.of("orange", 7))));

    PCollection<KV<String, Iterable<Integer>>> grouped =
        sales.apply("GroupByKey", GroupByKey.create());

    grouped.apply("PrintGrouped", ParDo.of(new PrintFn<>("grouped=")));

    p.run().waitUntilFinish();
  }
}
