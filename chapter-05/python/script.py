# Chapter 05 - Python snippets extracted from the book manuscript
# NOTE: Snippets are separated as they appeared; you may need to adapt into a single runnable pipeline.

# --- Snippet 1: Python: In the Python SDK, you can use beam.Map(func) with a lambda or function. For example, to square each number in a
import apache_beam as beam

# --- Snippet 2: Python: In the Python SDK, you can use beam.Map(func) with a lambda or function. For example, to square each number in a
# Sample input: PCollection of numbers
with beam.Pipeline() as p:
    numbers = p | beam.Create([1, 2, 3, 4])
    # Use Map to square each number
    squares = numbers | beam.Map(lambda x: x * x)
    # Print results (for local runner demonstration)
    squares | beam.Map(print)


# --- Snippet 5: Python: Use beam.Filter(predicate). For example, to keep only even numbers from a PCollection:
with beam.Pipeline() as p:
    numbers = p | beam.Create([1, 2, 3, 4, 5, 6])
    evens = numbers | beam.Filter(lambda x: x % 2 == 0)
    evens | beam.Map(print)  # This will print 2, 4, 6


# --- Snippet 9: Python: In the Python SDK, use beam.FlatMap(func). The fun​ction returns an iterable (list, generator, etc.) of results 
with beam.Pipeline() as p:
    lines = p | beam.Create([
        "Apache Beam is unified",
        "Beam supports batch and streaming"
    ])
    # FlatMap: split each line into words
    words = lines | beam.FlatMap(lambda line: line.split(" "))
    words | beam.Map(print)

# --- Snippet 10: Python
def split_words(line):
    for word in line.split(" "):
        yield word

# --- Snippet 11: Python
words = lines | beam.FlatMap(split_words)


# --- Snippet 13: For example, this is problematic:
# BAD: using a dict as a key
data = p | beam.Create([
    ({"user": "alice"}, 10),
    ({"user": "alice"}, 20),
])
grouped = data | beam.GroupByKey()

# --- Snippet 14: The correct approach is to use simple, stable, primitive keys, such as strings, integers, or tuples of primitives:
# GOOD: use a stable primitive key
data = p | beam.Create([
    ("alice", 10),
    ("alice", 20),
])
grouped = data | beam.GroupByKey()

# --- Snippet 15: Or if your key is embedded inside a complex object, extract a deterministic field:
# GOOD: extract a stable field from a complex structure
data = p | beam.Create([
    ({"user": "alice", "region": "EU"}, 10),
    ({"user": "alice", "region": "EU"}, 20),
])

# --- Snippet 16
keyed = data | beam.Map(lambda x: (x[0]["user"], x[1]))
grouped = keyed | beam.GroupByKey()

# --- Snippet 17: Python: In Python, the transform is beam.GroupByKey(). Suppose we have a PCollection of key-value pairs (for example, fr
with beam.Pipeline() as p:
    sales = p | beam.Create([
        ("apple", 3),
        ("banana", 4),
        ("apple", 5),
        ("banana", 2),
        ("orange", 7)
    ])
    grouped = sales | beam.GroupByKey()
    grouped | beam.Map(print)


# --- Snippet 19: Python: In Python, CoGroupByKey expects a dictionary mapping tag names to PCollections. Each PCollection should consist 
with beam.Pipeline() as p:
    ages = p | "CreateAges" >> beam.Create([
        ("Alice", 25),
        ("Bob", 30)
    ])
    cities = p | "CreateCities" >> beam.Create([
        ("Alice", "NY"),
        ("Charles", "LA")
    ])
    # CoGroupByKey requires a dict of PCollections
    grouped = ({'ages': ages, 'cities': cities}
               | beam.CoGroupByKey())
    grouped | beam.Map(print)


# --- Snippet 21: Python: In Python, beam.Partition(fn, N) is used, and it returns a tuple of N PCollections. For example, let's partition
with beam.Pipeline() as p:
    numbers = p | beam.Create(list(range(10)))  # 0,1,2,...9
    def partition_fn(x, num_partitions):
        # Return 0 for even numbers, 1 for odd numbers
        return 0 if x % 2 == 0 else 1

# --- Snippet 22
    even_part, odd_part = numbers | beam.Partition(partition_fn, 2)
    # even_part will contain [0, 2, 4, 6, 8]
    # odd_part will contain [1, 3, 5, 7, 9]
    even_part | beam.Map(lambda x: f"even: {x}") | beam.Map(print)
    odd_part  | beam.Map(lambda x: f"odd: {x}")  | beam.Map(print)


# --- Snippet 25: Conceptually:
routed = pcoll | beam.Map(lambda x: (compute_route(x), x))
Then use a sink that supports dynamic destinations, such as FileIO.writeDynamic() in Java or WriteToText/custom sinks in Python with routing logic.

# --- Snippet 26: Let's implement this:
import apache_beam as beam

# --- Snippet 27
def split_words(line):
    # Split a line into words by whitespace
    for word in line.split():
        # Normalize the word by lowercasing and stripping punctuation (basic cleanup)
        yield word.lower().strip(".,!?")

# --- Snippet 28
with beam.Pipeline() as pipeline:
    # Create a PCollection of lines
    lines_pc = pipeline | "CreateLines" >> beam.Create(lines)
    # 2. Split lines into words
    words = lines_pc | "SplitWords" >> beam.FlatMap(split_words)
    # 3. Map each word to a (word, 1) pair
    word_ones = words | "AddCountTuple" >> beam.Map(lambda w: (w, 1))
    # 4. Group by word and sum counts
    grouped = word_ones | "GroupByWord" >> beam.GroupByKey()
    word_counts = grouped | "SumCounts" >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
    # 5. Print the results
    word_counts | "PrintResults" >> beam.Map(print)

# --- Snippet 29: Python
word_counts = words | beam.combiners.Count.PerElement()

# --- Snippet 30: Python
word_counts = words | beam.combiners.Count.PerElement()


# --- Snippet 32: Let's implement this in Python:
with beam.Pipeline() as p:
    # 1. PCollection of names to ages
    ages = p | "Ages" >> beam.Create([
        ("Alice", 30),
        ("Bob", 20),
        ("Charlie", 25)
    ])
    # 2. PCollection of names to favorite color
    colors = p | "Colors" >> beam.Create([
        ("Alice", "Blue"),
        ("Bob", "Green"),
        ("Diana", "Red")
    ])
    # 3. Join the two PCollections by name
    joined = ({'age': ages, 'color': colors}
              | "JoinAgesColors" >> beam.CoGroupByKey())
    # 4. Process joined results
    def format_result(name_info):
        name, info = name_info
        ages_list = info['age']
        colors_list = info['color']
        # Get the single age or None if not present
        age = ages_list[0] if ages_list else None
        # Get the single color or None if not present
        color = colors_list[0] if colors_list else None
        return (name, age, color)

# --- Snippet 33
    formatted = joined | "FormatResults" >> beam.Map(format_result)
    # 5. Print the output tuples
    formatted | beam.Map(print)

# --- Snippet 34: Using the DirectRunner explicitly: If you want to be explicit, you can set the runner to DirectRunner. In Python:
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DirectRunner'
p = beam.Pipeline(options=options)

# --- Snippet 35: Python: You specify Dataflow options via the PipelineOptions. Typically, you'd pass command-line arguments or construct 
options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=<YOUR_GCP_PROJECT_ID>",
    "--region=<YOUR_DATAFLOW_REGION>",        # e.g., "us-central1"
    "--temp_location=gs://<YOUR_BUCKET>/temp",
    "--job_name=beam-wordcount-example"
])
p = beam.Pipeline(options=options)

# --- Snippet 36: In code, you could also do:
options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(GoogleCloudOptions).project = 'YOUR_GCP_PROJECT_ID'
options.view_as(GoogleCloudOptions).region = 'us-central1'
options.view_as(GoogleCloudOptions).temp_location = 'gs://YOUR_BUCKET/temp'
options.view_as(GoogleCloudOptions).job_name = 'beam-wordcount-example'
p = beam.Pipeline(options=options)

