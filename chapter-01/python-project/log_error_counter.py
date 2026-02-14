import apache_beam as beam 

# Pipeline to count error messages from log files 
def is_error_log(line): 
    return 'ERROR' in line 

p = beam.Pipeline() 

# Read from a batch data source (e.g., a set of log files) 
lines = p | 'ReadLogs' >> beam.io.ReadFromText('logs/*.txt') 

# Filter lines to keep only error messages 
error_logs = lines | 'FilterErrors' >> beam.Filter(is_error_log) 

# Count the number of error messages 
error_count = error_logs | 'CountErrors' >> beam.combiners.Count.Globally() 

# Write the count to an output file 
error_count | 'WriteOutput' >> beam.io.WriteToText('error_counts.txt') 

p.run() 