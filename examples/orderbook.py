import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, List, Optional


import websockets
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async


async def _ws_agen(product_id):
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


class CoinbasePartition(StatefulSourcePartition):
    def __init__(self, product_id):
        agen = _ws_agen(product_id)
        self._batcher = batch_async(agen, timedelta(seconds=0.5), 100)

    def next_batch(self):
        return next(self._batcher)

    def snapshot(self):
        return None


@dataclass
class CoinbaseSource(FixedPartitionedSource):
    product_ids: List[str]

    def list_parts(self):
        return self.product_ids

    def build_part(self, step_id, for_key, _resume_state):
        return CoinbasePartition(for_key)


@dataclass(frozen=True)
class OrderBookSummary:
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    spread: float


@dataclass
class OrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None

    def update(self, data):
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

            # If size is zero, remove the price level; otherwise, update/add the price level
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
        return self.ask_price - self.bid_price  # type: ignore

    def summarize(self):
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
    if state is None:
        state = OrderBookState()

    state.update(value)
    return (state, state.summarize())


stats = op.stateful_map("orderbook", inp, mapper)
# ('BTC-USD', (36905.39, 0.00334873, 36905.4, 1.6e-05, 0.010000000002037268))


# # filter on 0.1% spread as a per
def just_large_spread(prod_summary):
    product, summary = prod_summary
    return summary.spread / summary.ask_price > 0.0001


state = op.filter("big_spread", stats, just_large_spread)
op.output("out", stats, StdOutSink())