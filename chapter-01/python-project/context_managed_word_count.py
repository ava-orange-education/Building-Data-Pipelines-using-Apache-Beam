import apache_beam as beam

with beam.Pipeline() as p:
    # Step 1: Read input from a text file
    lines = p | 'ReadFile' >> beam.io.ReadFromText('input.txt')

    # Step 2: Split each line into words
    words = lines | 'SplitWords' >> beam.FlatMap(lambda line: line.split())

    # Step 3: Assign each word a count of 1
    word_pairs = words | 'PairWordsWithOne' >> beam.Map(lambda word: (word, 1))

    # Step 4: Group words and sum their counts
    word_counts = word_pairs | 'CountWords' >> beam.CombinePerKey(sum)

    # Step 5: Write the results to an output file
    word_counts | 'WriteResults' >> beam.io.WriteToText('word_counts.txt')