# === Snippet ===

import json
import apache_beam as beam


# === Snippet ===
# Sample DoFn to parse a JSON event string into a Python dict
class ParseEventJson(beam.DoFn):
    def process(self, element):
        # element is a JSON string from the input
        record = json.loads(element)
        yield record


# === Snippet ===
# Pipeline setup (assuming pipeline options are configured elsewhere)
with beam.Pipeline() as p:
    # Read raw events from a source (could be a file or Pub/Sub subscription in real scenario)
    raw_events = p | "ReadEvents" >> beam.io.ReadFromText("events.json")  # Input source


# === Snippet ===
    # Parse JSON strings to dicts
    parsed_events = raw_events | "ParseJSON" >> beam.ParDo(ParseEventJson())


# === Snippet ===
    # **Filter early:** Only keep events that are completed orders
    orders = parsed_events | "FilterCompletedOrders" >> beam.Filter(
        lambda record: record.get("event_type") == "ORDER_COMPLETED"
    )


# === Snippet ===
    # Continue pipeline with only order events...
    # (For example, map to (product_category, order_amount) for aggregation later)
    order_kvs = orders | "ToCategoryAmountKV" >> beam.Map(
        lambda record: (record["product_category"], record["order_total"])
    )
    # ... further transforms ...


# === Snippet ===
# Continuing from the previous snippet where we have order_kvs = (category, order_total)
# Approach A: Group then sum
grouped_by_category = order_kvs | "GroupByCategory" >> beam.GroupByKey()
# Now each element is (category, Iterable[order_totals])
sums_per_category = grouped_by_category | "SumOrders" >> beam.Map(
    lambda kv: (kv[0], sum(kv[1]))
)


# === Snippet ===
# Continuing from order_kvs = (category, order_total)
sums_per_category = order_kvs | "SumOrdersByCategory" >> beam.CombinePerKey(sum)


# === Snippet ===
sums_per_category = order_kvs | "SumOrdersByCategory" >> beam.CombinePerKey(sum)


# === Snippet ===
result = (
    pcoll
    | beam.CombinePerKey(my_combine_fn).with_hot_key_fanout(5)
)


# === Snippet ===
import random


# === Snippet ===
class ShardHotCategory(beam.DoFn):
    def process(self, element, hot_category, num_shards):
        # element is a (category, value) tuple
        category, value = element
        if category == hot_category:
            # Assign a random shard number to this hot category
            shard = random.randint(0, num_shards - 1)
            # Key becomes "category:shard"
            yield (f"{category}_{shard}", value)
        else:
            # Other categories remain unchanged
            yield (category, value)
    def process(self, element, hot_category, num_shards):
        category, value = element
        if category == hot_category:
            shard = random.randint(0, num_shards - 1)
            # Composite key avoids string parsing issues
            yield ((category, shard), value)
        else:
            ### Use a single shard for non-hot keys (optional, but keeps key type consistent).If reproducibility matters, replace random.randint(...) with a deterministic shard choice (e.g., hash(event_id) % num_shards), or explicitly seed random in a controlled way. #####
            yield ((category, 0), value)


# === Snippet ===
# ... inside the pipeline ...
NUM_SHARDS = 5
hot_cat = "Electronics"
# shard the hot category key into 5
sharded_kvs = order_kvs | "ShardHotKey" >> beam.ParDo(ShardHotCategory(), hot_category=hot_cat, num_shards=NUM_SHARDS)


# === Snippet ===
# First level combine on sharded keys
partial_sums = sharded_kvs | "CombineShards" >> beam.CombinePerKey(sum)


# === Snippet ===
# Now combine shards back to original category by mapping the key
rekeyed = partial_sums | "UnshardKey" >> beam.Map(lambda kv: (kv[0].split("_")[0], kv[1]))
# Second level combine by original category (this final combine should be small since there are at most num_shards values per category)
final_sums = rekeyed | "FinalCombine" >> beam.CombinePerKey(sum)


# === Snippet ===
# Assume we have a PCollection of product info: (product_id, category)
products = p | "ReadProducts" >> beam.io.ReadFromText("products.csv") | beam.Map(lambda line: line.split(","))  # simplistic CSV read
products_kv = products | beam.Map(lambda fields: (fields[0], fields[1]))  # (product_id, category)


# === Snippet ===
# Convert products PCollection to a side input (as a dict for quick lookup)
products_side = beam.pvalue.AsDict(products_kv)


# === Snippet ===
# Now use this side input in a DoFn for orders
class AddCategory(beam.DoFn):
    def process(self, order, products_lookup):
        # order is a dict for an order event
        prod_id = order["product_id"]
        category = products_lookup.get(prod_id, "UNKNOWN")
        # add category info to the order record
        order["product_category"] = category
        yield order


# === Snippet ===
enriched_orders = orders | "AddCategoryInfo" >> beam.ParDo(AddCategory(), products_lookup=products_side)


# === Snippet ===
import apache_beam as beam
from dataclasses import dataclass
from datetime import datetime


# === Snippet ===
def parse_product(line: str):
    parts = [p.strip() for p in line.split(",")]
    product_id, category, updated_at_str = parts[0], parts[1], parts[2]
    # ISO-8601 parse; adjust if your timestamp format differs
    ts = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00")).timestamp()
    return product_id, {"category": category, "ts": ts}


# === Snippet ===
def latest_record(a: dict, b: dict) -> dict:
    # "latest wins"
    return a if a["ts"] >= b["ts"] else b


# === Snippet ===
with beam.Pipeline() as p:
    # Read & parse products
    products = (
        p
        | "ReadProducts" >> beam.io.ReadFromText("products.csv", skip_header_lines=1)
        | "ParseProducts" >> beam.Map(parse_product)  # (product_id, {"category":..,"ts":..})
    )


# === Snippet ===
    # Deduplicate so each product_id has exactly one record (latest wins)
    products_latest = (
        products
        | "LatestPerProduct" >> beam.CombinePerKey(latest_record)  # (product_id, {"category","ts"})
        | "ToLookupKV" >> beam.Map(lambda kv: (kv[0], kv[1]["category"]))  # (product_id, category)
    )


# === Snippet ===
    # Safe AsDict now (unique keys)
    products_side = beam.pvalue.AsDict(products_latest)


# === Snippet ===
    # Example orders stream (for demo purposes we use Create, but this could come from Kafka #/ PubSub in streaming)
orders = (
    p
    | "CreateOrders" >> beam.Create([
        {
            "order_id": "O1001",
            "product_id": "P123",
            "user_id": "U001",
            "amount": 1500.00
        },
        {
            "order_id": "O1002",
            "product_id": "P456",
            "user_id": "U002",
            "amount": 2300.00
        },
        {
            "order_id": "O1003",
            "product_id": "P999",  # product not in products.csv
            "user_id": "U003",
            "amount": 799.00
        }
    ])
)
    class AddCategory(beam.DoFn):
        def process(self, order: dict, products_lookup: dict):
            prod_id = order.get("product_id")
            category = products_lookup.get(prod_id, "UNKNOWN")
            # Avoid mutating shared objects; create a copy
            out = dict(order)
            out["product_category"] = category
            yield out


# === Snippet ===
    enriched_orders = orders | "AddCategoryInfo" >> beam.ParDo(AddCategory(), products_lookup=products_side)


# === Snippet ===
    # For a real pipeline, write to a sink instead of print()
    # enriched_orders | beam.io.WriteToText("output/enriched_orders")


# === Snippet ===
beam.io.ReadFromText(
    file_pattern,
    compression_type=beam.io.filesystem.CompressionTypes.AUTO,
    min_bundle_size=0
)


# === Snippet ===
class BatchValidateAddress(beam.DoFn):
    def __init__(self):
        self.buffer = []
        self.MAX_BATCH_SIZE = 50
    def process(self, element):
        # Add element (order) to buffer
        self.buffer.append(element)
        # If buffer reached batch size, flush (make API call)
        if len(self.buffer) >= self.MAX_BATCH_SIZE:
            self._flush()
        # No yield here; we yield in flush after getting results.
    def finish_bundle(self):
        # Called at end of bundle or pipeline teardown; flush remaining
        if self.buffer:
            self._flush()
    def _flush(self):
        batch = self.buffer
        self.buffer = []
        # Imagine we call an external API with this batch
        results = call_address_api(batch)  # returns a list of responses equal to batch size
        # Pair each order with its result and yield
        for order, validation in zip(batch, results):
            order["address_valid"] = validation
            yield order


# === Snippet ===
import apache_beam as beam


# === Snippet ===
# Example external service call (replace with your real implementation)
def call_address_api(orders_batch):


# === Snippet ===
class ValidateAddressBatch(beam.DoFn):
    def process(self, orders_batch):
        # Make ONE API call for the whole batch


# === Snippet ===
# Example pipeline usage
with beam.Pipeline() as p:
    orders = p | "CreateOrders" >> beam.Create([


# === Snippet ===
        # Batch into lists of elements (Beam chooses actual batch sizes within these bounds)
        | "BatchOrders" >> beam.BatchElements(min_batch_size=10, max_batch_size=50)
        # Call the external service once per batch
        | "ValidateAddressInBatches" >> beam.ParDo(ValidateAddressBatch())


# === Snippet ===
    validated_orders | "PrintForDemo" >> beam.Map(print)


# === Snippet ===
from apache_beam.metrics import Metrics
class ProcessOrder(beam.DoFn):
    def __init__(self):
        # Define a counter metric
        self.order_counter = Metrics.counter("ecommerce", "orders_processed")
        self.large_order_counter = Metrics.counter("ecommerce", "large_orders")
    def process(self, order):
        # Increment counter for every order
        self.order_counter.inc()
        # If order total is large, count it separately
        if order.get("order_total", 0) > 1000:
            self.large_order_counter.inc()
        # (Do some processing on the order, e.g., format or validate)
        yield order


# === Snippet ===
# Use the DoFn in pipeline
processed = orders | "ProcessOrder" >> beam.ParDo(ProcessOrder())


# === Snippet ===
class CategoryMetrics(beam.DoFn):
    def __init__(self):
        self.counters = {}
    def process(self, kv):
        category, value = kv
        # Lazily create a counter for this category
        if category not in self.counters:
            self.counters[category] = Metrics.counter("orders_by_category", category)
        self.counters[category].inc()
        yield kv  # pass data through
