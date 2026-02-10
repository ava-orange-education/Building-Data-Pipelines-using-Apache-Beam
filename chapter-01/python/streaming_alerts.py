import apache_beam as beam

# Function to check for high temperatures
def check_high_temperature(reading):
    timestamp, temperature = reading.split(',')
    if float(temperature) > 75.0:
        return True 
    return False

p = beam.Pipeline() 

# Simulate a streaming data source (e.g., from IoT sensors) 
temperature_data = p | 'ReadTemperatureStream' >> beam.io.ReadFromPubSub(topic='projects/my-project/topics/temperature') 

# Filter readings where the temperature exceeds 75.0 degrees 
high_temp_readings = temperature_data | 'FilterHighTemp' >> beam.Filter(check_high_temperature)

# Write the high temperature readings to an alert system 
high_temp_readings | 'WriteToAlerts' >> beam.io.WriteToText('high_temp_alerts.txt') 

p.run() 