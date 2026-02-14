package com.example.chapter05;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class FilterLongWordsExample {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    PCollection<String> words = p.apply("CreateWords", Create.of("hello", "hi", "world", "beam"));

    PCollection<String> longWords =
        words.apply("FilterLongWords", Filter.by((String word) -> word.length() > 3));

    longWords.apply("PrintLongWords", ParDo.of(new PrintFn<>("longWord=")));

    p.run().waitUntilFinish();
  }
}
