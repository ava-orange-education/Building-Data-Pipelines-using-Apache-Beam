import apache_beam as beam
from apache_beam.metrics import Metrics


def transform(x):
    return str(x).upper()


class MyDoFn(beam.DoFn):
    def __init__(self):
        self.processed_count = Metrics.counter(self.__class__, "processed_count")
        self.length_distribution = Metrics.distribution(self.__class__, "element_length")

    def process(self, element):
        self.processed_count.inc()
        self.length_distribution.update(len(str(element)))
        yield transform(element)


def run():
    with beam.Pipeline() as p:
        (
            p
            | "Create" >> beam.Create(["a", "bb", "ccc"])
            | "Process" >> beam.ParDo(MyDoFn())
            | "Print" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
