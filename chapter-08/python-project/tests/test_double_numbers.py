import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


def test_double_numbers():
    nums = [1, 2, 3, 4]
    with TestPipeline() as p:
        input_collection = p | beam.Create(nums)
        output_collection = input_collection | beam.Map(lambda x: x * 2)
        assert_that(output_collection, equal_to([x * 2 for x in nums]))
