import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple

import apache_beam as beam
from apache_beam.metrics import Metrics


# ----------------------------------------------------------------------
# 1) Early Filtering Optimization: Drop Irrelevant Events at Ingestion Time
# ----------------------------------------------------------------------
class ParseEventJson(beam.DoFn):
    """Parse a JSON event string into a Python dict."""

    def process(self, element: str):
        record = json.loads(element)
        yield record


def demo_early_filtering():
    print("\n=== Demo 1: Early filtering ===")

    # For a runnable demo we use Create(); the chapter used ReadFromText("events.json")
    raw_events = [
        json.dumps({"event_type": "PAGE_VIEW", "product_category": "Shoes", "order_total": 0}),
        json.dumps({"event_type": "ORDER_COMPLETED", "product_category": "Electronics", "order_total": 1500}),
        json.dumps({"event_type": "ORDER_COMPLETED", "product_category": "Books", "order_total": 45}),
    ]

    with beam.Pipeline() as p:
        events = p | "CreateRawEvents" >> beam.Create(raw_events)

        parsed = events | "ParseJSON" >> beam.ParDo(ParseEventJson())

        orders = parsed | "FilterCompletedOrders" >> beam.Filter(
            lambda record: record.get("event_type") == "ORDER_COMPLETED"
        )

        order_kvs = orders | "ToCategoryAmountKV" >> beam.Map(
            lambda record: (record["product_category"], record["order_total"])
        )

        order_kvs | "PrintOrderKVs" >> beam.Map(lambda kv: print("order_kv=", kv))


# ----------------------------------------------------------------------
# 2) Preparing Side Inputs Safely: Deduplication with CombinePerKey
# ----------------------------------------------------------------------
def parse_product(line: str):
    """Parse a CSV line into (product_id, {category, ts})."""
    parts = [p.strip() for p in line.split(",")]
    product_id, category, updated_at_str = parts[0], parts[1], parts[2]
    ts = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00")).timestamp()
    return product_id, {"category": category, "ts": ts}


def latest_record(a: dict, b: dict) -> dict:
    """CombineFn for 'latest wins' by timestamp."""
    return a if a["ts"] >= b["ts"] else b


class AddCategory(beam.DoFn):
    def process(self, order: dict, products_lookup: Dict[str, str]):
        prod_id = order.get("product_id")
        category = products_lookup.get(prod_id, "UNKNOWN")
        out = dict(order)  # avoid mutating shared objects
        out["product_category"] = category
        yield out


def demo_side_input_dedup_and_join():
    print("\n=== Demo 2: Side input dedup + join ===")

    # Simulated products.csv (with duplicates)
    products_csv = [
        "product_id,category,updated_at",
        "P123,Electronics,2026-01-10T08:15:00Z",
        "P123,Electronics,2026-01-11T09:00:00Z",  # newer
        "P456,Home,2026-01-09T10:00:00Z",
    ]

    orders_list = [
        {"order_id": "O1001", "product_id": "P123", "user_id": "U001", "amount": 1500.00},
        {"order_id": "O1002", "product_id": "P456", "user_id": "U002", "amount": 2300.00},
        {"order_id": "O1003", "product_id": "P999", "user_id": "U003", "amount": 799.00},  # missing product
    ]

    with beam.Pipeline() as p:
        products = (
            p
            | "CreateProductsCSV" >> beam.Create(products_csv)
            | "SkipHeader" >> beam.Filter(lambda line: not line.startswith("product_id,"))
            | "ParseProducts" >> beam.Map(parse_product)
        )

        products_latest = (
            products
            | "LatestPerProduct" >> beam.CombinePerKey(latest_record)
            | "ToLookupKV" >> beam.Map(lambda kv: (kv[0], kv[1]["category"]))
        )

        products_side = beam.pvalue.AsDict(products_latest)

        orders = p | "CreateOrders" >> beam.Create(orders_list)

        enriched = orders | "AddCategoryInfo" >> beam.ParDo(AddCategory(), products_lookup=products_side)

        enriched | "PrintEnrichedOrders" >> beam.Map(lambda rec: print("enriched=", rec))


# ----------------------------------------------------------------------
# 3) External I/O Optimization: Batching Remote API Calls with BatchElements
# ----------------------------------------------------------------------
def call_address_api(orders_batch: List[dict]) -> List[bool]:
    """Placeholder external service call. Replace with real implementation."""
    # Example logic: mark all as valid
    return [True for _ in orders_batch]


class ValidateAddressBatch(beam.DoFn):
    def process(self, orders_batch: List[dict]):
        validations = call_address_api(orders_batch)
        for order, is_valid in zip(orders_batch, validations):
            enriched = dict(order)
            enriched["address_valid"] = is_valid
            yield enriched


def demo_batch_external_io():
    print("\n=== Demo 3: Batch external I/O calls ===")

    with beam.Pipeline() as p:
        orders = p | "CreateOrdersForValidation" >> beam.Create(
            [
                {"order_id": "o1", "address": "1 Main St"},
                {"order_id": "o2", "address": "2 High St"},
                {"order_id": "o3", "address": "3 Lake Rd"},
            ]
        )

        validated = (
            orders
            | "BatchOrders" >> beam.BatchElements(min_batch_size=2, max_batch_size=50)
            | "ValidateAddressInBatches" >> beam.ParDo(ValidateAddressBatch())
        )

        validated | "PrintValidated" >> beam.Map(lambda rec: print("validated=", rec))


# ----------------------------------------------------------------------
# 4) Pipeline Monitoring: Using Beam Metrics for Observability
# ----------------------------------------------------------------------
class ProcessOrder(beam.DoFn):
    def __init__(self):
        self.order_counter = Metrics.counter("ecommerce", "orders_processed")
        self.large_order_counter = Metrics.counter("ecommerce", "large_orders")

    def process(self, order: dict):
        self.order_counter.inc()
        if order.get("order_total", 0) > 1000:
            self.large_order_counter.inc()
        yield order


def demo_metrics():
    print("\n=== Demo 4: Metrics counters ===")

    orders = [
        {"order_id": "o100", "order_total": 50},
        {"order_id": "o101", "order_total": 2500},
        {"order_id": "o102", "order_total": 1200},
    ]

    with beam.Pipeline() as p:
        processed = p | "CreateOrdersForMetrics" >> beam.Create(orders) | "ProcessOrder" >> beam.ParDo(ProcessOrder())
        processed | "PrintProcessed" >> beam.Map(lambda rec: print("processed=", rec))


if __name__ == "__main__":
    demo_early_filtering()
    demo_side_input_dedup_and_join()
    demo_batch_external_io()
    demo_metrics()
