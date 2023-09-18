import json
from collections import OrderedDict
from dataclasses import dataclass
from datetime import timedelta
from typing import List

# pip install websockets
import websockets
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.operators import UnaryLogic


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

    def build_part(self, for_key, _resume_state):
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
    bids: OrderedDict
    asks: OrderedDict
    bid_price: float
    ask_price: float

    def update(self, data):
        # We receive a list of lists here, normally it is only one change,
        # but could be more than one.
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
        return self.ask_price - self.bid_price

    def summarize(self):
        return OrderBookSummary(
            bid_price=self.bid_price,
            bid_size=self.bids[self.bid_price],
            ask_price=self.ask_price,
            ask_size=self.asks[self.ask_price],
            spread=self.spread(),
        )


class OrderBookLogic(UnaryLogic):
    def __init__(self, state):
        self.state = state

    def on_item(self, value):
        if self.state is None:
            bids = OrderedDict(
                (float(price), float(size)) for price, size in value["bids"]
            )
            # The bid_price is the highest priced buy limit order.
            # since the bids are in order, the first item of our newly constructed bids
            # will have our bid price, so we can track the best bid
            bid_price = next(iter(bids))
            asks = OrderedDict(
                (float(price), float(size)) for price, size in value["asks"]
            )
            # The ask price is the lowest priced sell limit order.
            # since the asks are in order, the first item of our newly constructed
            # asks will be our ask price, so we can track the best ask
            ask_price = next(iter(asks))
            self.state = OrderBookState(bids, asks, bid_price, ask_price)
        else:
            self.state.update(value)

        return [self.state.summarize()]

    def on_eof(self):
        return []

    def is_complete(self):
        return False

    def snapshot(self):
        return self.state


flow = Dataflow("orderbook")
inp = flow.input("input", CoinbaseSource(["BTC-USD", "ETH-USD", "BTC-EUR", "ETH-EUR"]))
# ('BTC-USD', {
#     'type': 'l2update',
#     'product_id': 'BTC-USD',
#     'changes': [['buy', '36905.39', '0.00334873']],
#     'time': '2022-05-05T17:25:09.072519Z',
# })
stats = inp.unary("order_book", OrderBookLogic)
# ('BTC-USD', (36905.39, 0.00334873, 36905.4, 1.6e-05, 0.010000000002037268))

# filter on 0.1% spread as a per
def just_large_spread(prod_summary):
    product, summary = prod_summary
    return summary.spread / summary.ask_price > 0.0001


stats.filter("big_spread", just_large_spread).output("out", StdOutSink())
