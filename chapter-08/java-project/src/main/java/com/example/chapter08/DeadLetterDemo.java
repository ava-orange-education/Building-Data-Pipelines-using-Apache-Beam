package com.example.chapter08;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class DeadLetterDemo {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    PCollection<String> jsonLines =
        p.apply(
            "CreateJson",
            Create.of(
                Arrays.asList(
                    "{\"id\":\"1\", \"payload\":\"ok\"}",
                    "{\"id\":\"2\", \"payload\":\"also_ok\"}",
                    "{\"id\":\"3\", \"payload\":", // malformed
                    "not even json" // malformed
                    )));

    TupleTag<MyRecord> mainTag = new TupleTag<MyRecord>() {};

    PCollectionTuple out =
        jsonLines.apply(
            "ParseWithDeadLetter",
            ParDo.of(new ParseJsonDoFn())
                .withOutputTags(mainTag, TupleTagList.of(ParseJsonDoFn.DEADLETTER_TAG)));

    PCollection<MyRecord> good = out.get(mainTag);
    PCollection<String> bad = out.get(ParseJsonDoFn.DEADLETTER_TAG);

    good.apply("PrintGood", ParDo.of(new PrintToStdout<>("GOOD: ")));
    bad.apply("PrintBad", ParDo.of(new PrintToStdout<>("DEADLETTER: ")));

    p.run().waitUntilFinish();
  }

  static class PrintToStdout<T> extends org.apache.beam.sdk.transforms.DoFn<T, Void> {
    private final String prefix;

    PrintToStdout(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(@Element T e) {
      System.out.println(prefix + e);
    }
  }
}
