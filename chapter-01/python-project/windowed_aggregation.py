import apache_beam as beam
from apache_beam.transforms.window import FixedWindows

# Pipeline that processes streaming sensor data with fixed windowing
p = beam.Pipeline()

# Ingest data from a streaming source
sensor_data = p | 'ReadSensorData' >> beam.io.ReadFromPubSub(topic='projects/my-project/topics/sensor')

# Apply fixed windowing (e.g., 5-minute windows)
windowed_data = sensor_data | 'ApplyWindow' >> beam.WindowInto(FixedWindows(300))

# Perform transformation (calculate average reading per window)
avg_readings = windowed_data | 'Average' >> beam.CombineGlobally(beam.combiners.MeanCombineFn()).without_defaults()

# Write the results to an output sink
avg_readings | 'WriteToStorage' >> beam.io.WriteToText('average_readings.txt')

p.run()