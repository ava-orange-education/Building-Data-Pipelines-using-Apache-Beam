package com.example.chapter03;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/** Simple logging sink for the DirectRunner demos. */
public class Log {
  public static <T> PTransform<PCollection<T>, PCollection<T>> of(String label) {
    return new PTransform<>() {
      @Override
      public PCollection<T> expand(PCollection<T> input) {
        input.apply(label, ParDo.of(new DoFn<T, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            System.out.println("[" + label + "] " + c.element());
          }
        }));
        return input;
      }
    };
  }
}
