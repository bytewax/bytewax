import json
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Optional

# pip install websockets
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

    def next_batch(self, _sched):
        return next(self._batcher)

    def snapshot(self):
        return None


@dataclass
class CoinbaseSource(FixedPartitionedSource):
    product_ids: List[str]

    def list_parts(self):
        return self.product_ids

    def build_part(self, _step_id, for_key, _resume_state):
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
    bids: OrderedDict = field(default_factory=OrderedDict)
    asks: OrderedDict = field(default_factory=OrderedDict)
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None

    def update(self, data):
        if len(self.bids) <= 0:
            self.bids = OrderedDict(
                (float(price), float(size)) for price, size in data["bids"]
            )
        # The bid_price is the highest priced buy limit order.
        # since the bids are in order, the first item of our newly constructed bids
        # will have our bid price, so we can track the best bid
        if self.bid_price is None:
            self.bid_price = next(iter(self.bids))
        if len(self.asks) <= 0:
            self.asks = OrderedDict(
                (float(price), float(size)) for price, size in data["asks"]
            )
        # The ask price is the lowest priced sell limit order.
        # since the asks are in order, the first item of our newly constructed
        # asks will be our ask price, so we can track the best ask
        if self.ask_price is None:
            self.ask_price = next(iter(self.asks))

        # We receive a list of lists here, normally it is only one change,
        # but could be more than one.
        if "changes" in data:
            for update in data["changes"]:
                price = float(update[1])
                size = float(update[2])
            if update[0] == "sell":
                # first check if the size is zero and needs to be removed
                if size == 0.0:
                    try:
                        del self.asks[price]
                        # if it was the ask price removed,
                        # update with new ask price
                        if price <= self.ask_price:
                            self.ask_price = min(self.asks.keys())
                    except KeyError:
                        # don't need to add price with size zero
                        pass
                else:
                    self.asks[price] = size
                    if price < self.ask_price:
                        self.ask_price = price
            if update[0] == "buy":
                # first check if the size is zero and needs to be removed
                if size == 0.0:
                    try:
                        del self.bids[price]
                        # if it was the bid price removed,
                        # update with new bid price
                        if price >= self.bid_price:
                            self.bid_price = max(self.bids.keys())
                    except KeyError:
                        # don't need to add price with size zero
                        pass
                else:
                    self.bids[price] = size
                    if price > self.bid_price:
                        self.bid_price = price

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


stats = op.stateful_map("order_book", inp, mapper)
# ('BTC-USD', (36905.39, 0.00334873, 36905.4, 1.6e-05, 0.010000000002037268))


# filter on 0.1% spread as a per
def just_large_spread(prod_summary):
    product, summary = prod_summary
    return summary.spread / summary.ask_price > 0.0001


state = op.filter("big_spread", stats, just_large_spread)
op.output("out", stats, StdOutSink())
