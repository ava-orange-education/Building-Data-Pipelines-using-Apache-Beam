package com.example.chapter08;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class DoubleNumbersTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  public void doublesNumbers() {
    List<Integer> nums = Arrays.asList(1, 2, 3, 4);

    PCollection<Integer> out =
        p.apply(Create.of(nums))
            .apply(MapElements.into(TypeDescriptors.integers()).via(x -> x * 2));

    PAssert.that(out).containsInAnyOrder(2, 4, 6, 8);

    p.run().waitUntilFinish();
  }
}
