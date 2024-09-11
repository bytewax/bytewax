"""This script demonstrates how to use stateless operators in a dataflow."""

# start-imports
from pathlib import Path

import bytewax.operators as op
from bytewax.connectors.files import CSVSource
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulBatchLogic

# end-imports

# start-dataflow
# Initialize the dataflow
flow = Dataflow("init_smoothie")
# end-dataflow

# start-input
# Create the input source from the CSV file
orders = op.input("orders", flow, CSVSource(Path("smoothie_orders.csv")))
op.inspect("see_data", orders)
# end-input


# start-filter
def contains_banana(order):
    """Find orders that contain bananas and almond milk."""
    return "Banana" in order["ingredients"] and "Almond Milk" in order["ingredients"]


banana_orders = op.filter("filter_banana", orders, contains_banana)

op.inspect("filter_results", banana_orders)
# end-filter


# start-enrich
def mock_pricing_service(smoothie_name):
    """Mock pricing service to get the price of a smoothie."""
    prices = {
        "Green Machine": 5.99,
        "Berry Blast": 6.49,
        "Tropical Twist": 7.49,
        "Protein Power": 8.99,
        "Citrus Zing": 6.99,
        "Mocha Madness": 7.99,
        "Morning Glow": 5.49,
        "Nutty Delight": 8.49,
    }
    return prices.get(smoothie_name, 0)


def enrich_with_price(cache, order):
    """Enrich the order with the price of the smoothie."""
    order["price"] = cache.get(order["order_requested"])
    return order


# Enrich the data to add the price
enriched_orders = op.enrich_cached(
    "enrich_with_price", orders, mock_pricing_service, enrich_with_price
)

op.inspect("enrich_results", enriched_orders)
# end-enrich

# start-map
TAX_RATE = 0.15  # Example tax rate of 15%


def calculate_total_price(order):
    """Calculate the total price of the order including tax."""
    # Assuming each order has a quantity of 1 for simplicity
    order["total_price"] = round(float(order["price"]) * (1 + TAX_RATE), ndigits=2)
    return order


total_price_orders = op.map(
    "calculate_total_price", enriched_orders, calculate_total_price
)

op.inspect("inspect_final", total_price_orders)
# end-map

# start-count-final
counted_orders = op.count_final(
    "count_smoothies", total_price_orders, key=lambda x: x["order_requested"]
)

# op.inspect("inspect_final_count", counted_orders)
# end-count-final


# start-total-revenue
def calculate_total_revenue_with_tax(counts, pricing_service):
    """Multiply the count of smoothies by the cached price and include tax."""
    smoothie, count = counts
    price = pricing_service(smoothie)
    total_without_tax = count * price
    total_with_tax = round(total_without_tax * (1 + TAX_RATE), ndigits=2)  # Apply tax
    return (smoothie, total_with_tax)


# Use map operator to calculate total revenue
total_revenue_orders = op.map(
    "calculate_total_revenue",
    counted_orders,
    lambda counts: calculate_total_revenue_with_tax(counts, mock_pricing_service),
)

# Inspect the final total revenue per smoothie type
# op.inspect_debug("inspect_total_revenue", total_revenue_orders)
# end-total-revenue


# start-stateful
class PriceSumLogic(StatefulBatchLogic):
    """Stateful logic to sum the total price of smoothies."""

    def __init__(self):
        """Initialize the total price."""
        self.total_price = 0

    def on_batch(self, batch):
        """Sum the total price of smoothies in the batch."""
        for event in batch:
            key = event["order_requested"]
            self.total_price += event["total_price"]

        return [(key, round(self.total_price, ndigits=2))], False

    def snapshot(self):
        """Snapshot the total price."""
        return self.total_price

    def restore(self, snapshot):
        """Restore the total price from the snapshot."""
        self.total_price = snapshot


# Extract the smoothie name as key for stateful logic
keyed_orders = op.key_on(
    "key_smoothie", total_price_orders, lambda x: x["order_requested"]
)

op.inspect("keyed_orders", keyed_orders)

# Apply stateful batch processing
stateful_orders = op.stateful_batch(
    "stateful_batch_price", keyed_orders, lambda _: PriceSumLogic()
)

# # Redistribute after stateful processing
# redistributed_orders = op.redistribute("redistribute", stateful_orders)

op.inspect_debug("stateful_output", stateful_orders)
# end-stateful
