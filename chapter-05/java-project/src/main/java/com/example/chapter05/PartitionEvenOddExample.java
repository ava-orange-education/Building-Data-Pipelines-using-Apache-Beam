package com.example.chapter05;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PartitionEvenOddExample {
  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();

    PCollection<Integer> numbers =
        pipeline.apply("CreateNumbers", Create.of(1,2,3,4,5,6,7,8,9,10));

    PCollectionList<Integer> partitions =
        numbers.apply(
            "PartitionEvenOdd",
            Partition.of(2, (Integer n, int numPartitions) -> (n % 2 == 0) ? 0 : 1)
        );

    PCollection<Integer> evens = partitions.get(0);
    PCollection<Integer> odds  = partitions.get(1);

    PCollection<Integer> evenSum =
        evens.apply("SumEvens", Combine.globally(Sum.ofIntegers()).withoutDefaults());
    PCollection<Integer> oddSum =
        odds.apply("SumOdds", Combine.globally(Sum.ofIntegers()).withoutDefaults());

    evenSum
        .apply("FormatEvenSum", MapElements.into(TypeDescriptors.strings())
            .via(sum -> "Sum of evens: " + sum))
        .apply("PrintEvenSum", ParDo.of(new DoFn<String, Void>() {
          @ProcessElement
          public void processElement(@Element String msg) {
            System.out.println(msg);
          }
        }));

    oddSum
        .apply("FormatOddSum", MapElements.into(TypeDescriptors.strings())
            .via(sum -> "Sum of odds: " + sum))
        .apply("PrintOddSum", ParDo.of(new DoFn<String, Void>() {
          @ProcessElement
          public void processElement(@Element String msg) {
            System.out.println(msg);
          }
        }));

    pipeline.run().waitUntilFinish();
  }
}
