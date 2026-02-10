import apache_beam as beam

# Define a function to split lines into words 
def split_lines(line): 
    return line.split()

# Initialize the pipeline
pipeline = beam.Pipeline() 

# Define the pipeline steps 
word_counts = (pipeline 
    | 'ReadFromText' >> beam.io.ReadFromText('input.txt') 
    | 'SplitIntoWords' >> beam.FlatMap(split_lines) 
    | 'PairWithOne' >> beam.Map(lambda word: (word, 1)) 
    | 'CountPerWord' >> beam.CombinePerKey(sum) 
    | 'WriteToText' >> beam.io.WriteToText('output.txt')) 

# Run the pipeline 
pipeline.run() 