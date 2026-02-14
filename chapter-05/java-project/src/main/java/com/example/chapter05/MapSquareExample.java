package com.example.chapter05;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapSquareExample {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    PCollection<Integer> numbers = p.apply("CreateNumbers", Create.of(1, 2, 3, 4));

    PCollection<Integer> squares =
        numbers.apply(
            "SquareNumbers",
            MapElements.into(TypeDescriptors.integers()).via((Integer x) -> x * x));

    squares.apply("PrintSquares", ParDo.of(new PrintFn<>("square=")));

    p.run().waitUntilFinish();
  }
}
