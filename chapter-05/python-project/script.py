import apache_beam as beam
from apache_beam import window, trigger


def split_words(line: str):
    for word in line.split():
        yield word.lower().strip(".,!?")


def demo_map_square():
    print("\n--- Map: square numbers ---")
    with beam.Pipeline() as p:
        numbers = p | "CreateNumbers" >> beam.Create([1, 2, 3, 4])
        squares = numbers | "Square" >> beam.Map(lambda x: x * x)
        squares | "PrintSquares" >> beam.Map(lambda x: print(f"square={x}"))


def demo_flatmap_split():
    print("\n--- FlatMap: split lines into words ---")
    with beam.Pipeline() as p:
        lines = p | "CreateLines" >> beam.Create(
            ["Apache Beam is unified", "Beam supports batch and streaming"]
        )
        words = lines | "SplitWords" >> beam.FlatMap(lambda line: line.split(" "))
        words | "PrintWords" >> beam.Map(print)


def demo_wordcount_groupbykey():
    print("\n--- WordCount: Map + FlatMap + GroupByKey ---")
    lines = [
        "Apache Beam is great",
        "Beam unifies batch and streaming",
        "Streaming data is processed with Beam",
    ]
    with beam.Pipeline() as pipeline:
        lines_pc = pipeline | "CreateLines" >> beam.Create(lines)
        words = lines_pc | "SplitWords" >> beam.FlatMap(split_words)
        word_ones = words | "AddCountTuple" >> beam.Map(lambda w: (w, 1))
        grouped = word_ones | "GroupByWord" >> beam.GroupByKey()
        word_counts = grouped | "SumCounts" >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
        word_counts | "PrintResults" >> beam.Map(print)


def demo_groupbykey_with_window_and_triggers():
    print("\n--- GroupByKey: window + triggers (illustrative) ---")

    # Example KV events: (user_id, click_count_increment)
    events_data = [("u1", 1), ("u1", 1), ("u2", 1), ("u1", 1), ("u2", 1)]

    with beam.Pipeline() as p:
        events = p | "CreateEvents" >> beam.Create(events_data)

        grouped = (
            events
            | "WindowInto1Min"
            >> beam.WindowInto(
                window.FixedWindows(60),
                trigger=trigger.AfterWatermark(
                    early=trigger.AfterProcessingTime(10)
                ),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=60,
            )
            | "GroupByUser" >> beam.GroupByKey()
        )

        totals = grouped | "SumPerUser" >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
        totals | "PrintTotals" >> beam.Map(print)


if __name__ == "__main__":
    demo_map_square()
    demo_flatmap_split()
    demo_wordcount_groupbykey()
    demo_groupbykey_with_window_and_triggers()
