"""Setup a dataflow to process orderbook data from the Coinbase websocket.

This script sets up a dataflow to process orderbook data from the Coinbase
websocket. It connects to the websocket and subscribes to the level2_batch
channel for the given product_ids. It then processes the messages as they
arrive and calculates the best bid and ask prices, as well as the spread.
"""

# start-imports
import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, List, Optional

import websockets
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async

# end-imports


# start-async
async def _ws_agen(product_id):
    """Connect to websocket and yield messages as they arrive.

    This function is a generator that connects to the Coinbase websocket and
    yields messages as they arrive. It subscribes to the level2_batch channel for
    the given product_id.

    Args:
        product_id (_type_): The product_id to subscribe to.

    Yields:
        _type_: A tuple of the product_id and the message as a dictionary.
    """
    url = "wss://ws-feed.exchange.coinbase.com"
    async with websockets.connect(url) as websocket:
        msg = json.dumps(
            {
                "type": "subscribe",
                "product_ids": [product_id],
                "channels": ["level2_batch"],
            }
        )
        await websocket.send(msg)
        # The first msg is just a confirmation that we have subscribed.
        await websocket.recv()

        while True:
            msg = await websocket.recv()
            yield (product_id, json.loads(msg))


# end-async


class CoinbasePartition(StatefulSourcePartition):
    """Process messages from the Coinbase websocket as they arrive.

    This class is a partition that connects to the Coinbase websocket and
    yields messages as they arrive. It subscribes to the level2_batch channel for
    the given product_id.

    For more information on StatefulSourcePartition, see the documentation:
    https://docs.bytewax.io/stable/api/bytewax/bytewax.inputs.html#bytewax.inputs.StatefulSourcePartition

    Args:
        StatefulSourcePartition : The base class for a partition.
    """

    def __init__(self, product_id):
        """Initializes the partition with the given product_id.

        Args:
            product_id (str): The product_id to subscribe to.
        """
        agen = _ws_agen(product_id)
        self._batcher = batch_async(agen, timedelta(seconds=0.5), 100)

    def next_batch(self):
        """This function returns the next batch of messages from the websocket.

        Returns:
            _type_: A tuple of the product_id and the message as a dictionary.
        """
        return next(self._batcher)

    def snapshot(self):
        """This function returns a snapshot of the partition.

        Returns:
            _type_: _description_
        """
        return None


@dataclass
class CoinbaseSource(FixedPartitionedSource):
    """Connect to the Coinbase websocket and yield messages as they arrive.

    This class is a source that connects to the Coinbase websocket and
    yields messages as they arrive. It subscribes to the level2_batch channel for
    the given product_ids.

    Note that the methods associated with this class are required parts
    of the FixedPartitionedSource class.

    For more information on FixedPartitionedSource, see the documentation:
    https://docs.bytewax.io/stable/api/bytewax/bytewax.inputs.html#bytewax.inputs.FixedPartitionedSourceß

    Args:
        FixedPartitionedSource: The base class for a source.
    """

    product_ids: List[str]

    def list_parts(self):
        """This function returns the partitions for the source.

        Returns:
            List (str): The list of product_ids.
        """
        return self.product_ids

    def build_part(self, step_id, for_key, _resume_state):
        """This function builds a partition for the given product_id.

        Args:
            step_id (_type_): The step_id of the input operator.
            for_key (_type_): Which partition to build.
                                Will always be one of the keys returned
                                by list_parts on this worker.
            _resume_state (_type_): State data containing where in
                                    the input stream this partition should
                                    be begin reading during this execution.

        Returns:
            CoinbasePartition: The partition for the given product_id.
        """
        return CoinbasePartition(for_key)


@dataclass(frozen=True)
class OrderBookSummary:
    """Represents a summary of the order book state."""

    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    spread: float


@dataclass
class OrderBookState:
    """Maintains the state of the order book."""

    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None

    def update(self, data):
        """Update the order book state with the given data.

        Args:
            data: The data to update the order book state with.
        """
        # Initialize bids and asks if they're empty
        if not self.bids:
            self.bids = {float(price): float(size) for price, size in data["bids"]}
            self.bid_price = max(self.bids.keys(), default=None)
        if not self.asks:
            self.asks = {float(price): float(size) for price, size in data["asks"]}
            self.ask_price = min(self.asks.keys(), default=None)

        # Process updates from the "changes" field in the data
        for change in data.get("changes", []):
            side, price_str, size_str = change
            price, size = float(price_str), float(size_str)

            target_dict = self.asks if side == "sell" else self.bids

            # If size is zero, remove the price level; otherwise,
            # update/add the price level
            if size == 0.0:
                target_dict.pop(price, None)
            else:
                target_dict[price] = size

            # After update, recalculate the best bid and ask prices
            if side == "sell":
                self.ask_price = min(self.asks.keys(), default=None)
            else:
                self.bid_price = max(self.bids.keys(), default=None)

    def spread(self) -> float:
        """Calculate the spread between the best bid and ask prices.

        Returns:
            float: The spread between the best bid and ask prices.
        """
        return self.ask_price - self.bid_price  # type: ignore

    def summarize(self):
        """Summarize the order book state.

        Returns:
            OrderBookSummary: A summary of the order book state.
        """
        return OrderBookSummary(
            bid_price=self.bid_price,
            bid_size=self.bids[self.bid_price],
            ask_price=self.ask_price,
            ask_size=self.asks[self.ask_price],
            spread=self.spread(),
        )


flow = Dataflow("orderbook")
inp = op.input(
    "input", flow, CoinbaseSource(["BTC-USD", "ETH-USD", "BTC-EUR", "ETH-EUR"])
)
# ('BTC-USD', {
#     'type': 'l2update',
#     'product_id': 'BTC-USD',
#     'changes': [['buy', '36905.39', '0.00334873']],
#     'time': '2022-05-05T17:25:09.072519Z',
# })


def mapper(state, value):
    """Update the state with the given value and return the state and a summary."""
    if state is None:
        state = OrderBookState()

    state.update(value)
    return (state, state.summarize())


stats = op.stateful_map("orderbook", inp, mapper)
# ('BTC-USD', (36905.39, 0.00334873, 36905.4, 1.6e-05, 0.010000000002037268))


# # filter on 0.1% spread as a per
def just_large_spread(prod_summary):
    """Filter out products with a spread less than 0.1%."""
    product, summary = prod_summary
    return summary.spread / summary.ask_price > 0.0001


state = op.filter("big_spread", stats, just_large_spread)
op.output("out", stats, StdOutSink())
