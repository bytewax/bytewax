"""Setup a recoverable dataflow to join shopping cart events.

This example demonstrates how to use the bytewax library to create a recoverable
dataflow that processes a stream of shopping cart events. The dataflow uses a
stateful operator to join shopping cart events and summarize the shopping cart
state for each user.
"""

# start-imports
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from bytewax import operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

# end-imports

# start-dataflow
flow = Dataflow("shopping-cart-joiner")
input_data = op.input("input", flow, FileSource("cart-join.json"))
# end-dataflow


# start-deserialize
def safe_deserialize(data: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Deserialize JSON data and extract user_id, type, and order_id.

    Args:
        data: JSON data to deserialize.

    Returns:
        Tuple[str, dict]: A tuple containing the user_id and the event data.
    """
    try:
        event = json.loads(data)
        if "user_id" in event and "type" in event and "order_id" in event:
            return (event["user_id"], event)  # Return as (key, value) pair
    except json.JSONDecodeError:
        pass
    print(f"Skipping invalid data: {data}")
    return None


deserialize_data = op.filter_map("deserialize", input_data, safe_deserialize)
# end-deserialize


# start-state-class
@dataclass
class ShoppingCartState:
    """Class to maintain the state of the shopping cart."""

    unpaid_order_ids: dict = field(default_factory=dict)
    paid_order_ids: list = field(default_factory=list)

    def update(self, event):
        """Update the shopping cart state based on the event."""
        order_id = event["order_id"]
        if event["type"] == "order":
            self.unpaid_order_ids[order_id] = event
        elif event["type"] == "payment":
            if order_id in self.unpaid_order_ids:
                self.paid_order_ids.append(self.unpaid_order_ids.pop(order_id))

    def summarize(self):
        """Summarize the shopping cart state."""
        return {
            "paid_order_ids": [order["order_id"] for order in self.paid_order_ids],
            "unpaid_order_ids": list(self.unpaid_order_ids.keys()),
        }


def state_manager(state, value):
    """Update the shopping cart state and summarize the state."""
    if state is None:
        state = ShoppingCartState()
    state.update(value)
    return state, state.summarize()


# end-state-class

# start-map-manager
joined_data = op.stateful_map("joiner", deserialize_data, state_manager)

formatted_output = op.map(
    "format_output", joined_data, lambda x: f"Final summary for user {x[0]}: {x[1]}"
)
# end-map-manager

# start-output
op.output("output", formatted_output, StdOutSink())
# end-output
