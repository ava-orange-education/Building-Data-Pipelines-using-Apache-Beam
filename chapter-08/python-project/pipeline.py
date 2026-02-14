# === Snippet ===
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


# === Snippet ===
def double_numbers(nums):


# === Snippet ===
    with TestPipeline() as p:


 === Snippet ===
import apache_beam as beam
from apache_beam.metrics import Metrics


# === Snippet ===
class MyDoFn(beam.DoFn):
    def __init__(self):
        # Define a counter and distribution metric


# === Snippet ===
    def process(self, element):
        # Increment counter and update distribution


# === Snippet ===
class ValidateEventDoFn(beam.DoFn):
    def __init__(self):
        # Counter to count events missing user_id


# === Snippet ===
    def process(self, event):
        # event is a dict with possibly a 'user_id' field


# === Snippet ===
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
