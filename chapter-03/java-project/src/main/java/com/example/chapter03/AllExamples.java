package com.example.chapter03;

/**
 * Convenience entrypoint to run all Chapter 3 examples.
 *
 * Build:
 *   mvn -q -DskipTests package
 *
 * Run:
 *   java -jar target/chapter03-1.0.0-shaded.jar
 */
public class AllExamples {
  public static void main(String[] args) {
    System.out.println("=== AssignEventTimeWithTimestamps ===");
    AssignEventTimeWithTimestamps.run();

    System.out.println("=== WindowingExamples ===");
    WindowingExamples.run();

    System.out.println("=== TriggersExample ===");
    TriggersExample.run();
  }
}
