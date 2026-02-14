import apache_beam as beam
from apache_beam.transforms.userstate import BagStateSpec, TimerSpec
from apache_beam.coders import PickleCoder
from apache_beam.transforms.timeutil import TimeDomain

class StatefulDoFn(beam.DoFn):
    count_state = beam.DoFn.StateParam(
        beam.transforms.userstate.ReadModifyWriteStateSpec(
            name="count",
            coder=beam.coders.VarIntCoder()
        )
    )

    def process(self, element, count_state):
        key, value = element
        current = count_state.read() or 0
        current += value
        count_state.write(current)
        yield (key, current)