import json

from websocket import create_connection  # pip install websocket-client

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig

PRODUCT_IDS = ["BTC-USD", "ETH-USD", "SOL-USD"]


def ws_input(product_ids, state):
    ws = create_connection("wss://ws-feed.pro.coinbase.com")
    ws.send(
        json.dumps(
            {
                "type": "subscribe",
                "product_ids": product_ids,
                "channels": ["level2"],
            }
        )
    )
    # The first msg is just a confirmation that we have subscribed.
    print(ws.recv())
    while True:
        yield state, ws.recv()


def input_builder(worker_index, worker_count, resume_state):
    state = resume_state or None
    prods_per_worker = int(len(PRODUCT_IDS) / worker_count)
    product_ids = PRODUCT_IDS[
        int(worker_index * prods_per_worker) : int(
            worker_index * prods_per_worker + prods_per_worker
        )
    ]
    return ws_input(product_ids, state)


def key_on_product(data):
    return (data["product_id"], data)


class OrderBook:
    def __init__(self):
        # if using Python < 3.7 need to use OrderedDict here
        self.bids = {}
        self.asks = {}

    def update(self, data):
        if self.bids == {}:
            self.bids = {float(price): float(size) for price, size in data["bids"]}
            # The bid_price is the highest priced buy limit order.
            # since the bids are in order, the first item of our newly constructed bids
            # will have our bid price, so we can track the best bid
            self.bid_price = next(iter(self.bids))
        if self.asks == {}:
            self.asks = {float(price): float(size) for price, size in data["asks"]}
            # The ask price is the lowest priced sell limit order.
            # since the asks are in order, the first item of our newly constructed
            # asks will be our ask price, so we can track the best ask
            self.ask_price = next(iter(self.asks))
        else:
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
        return self, {
            "bid": self.bid_price,
            "bid_size": self.bids[self.bid_price],
            "ask": self.ask_price,
            "ask_price": self.asks[self.ask_price],
            "spread": self.ask_price - self.bid_price,
        }


flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(json.loads)
# {'type': 'l2update', 'product_id': 'BTC-USD', 'changes': [['buy', '36905.39', '0.00334873']], 'time': '2022-05-05T17:25:09.072519Z'}
flow.map(key_on_product)
# ('BTC-USD', {'type': 'l2update', 'product_id': 'BTC-USD', 'changes': [['buy', '36905.39', '0.00334873']], 'time': '2022-05-05T17:25:09.072519Z'})
flow.stateful_map("order_book", lambda: OrderBook(), OrderBook.update)
# ('BTC-USD', (36905.39, 0.00334873, 36905.4, 1.6e-05, 0.010000000002037268))
flow.filter(
    lambda x: x[-1]["spread"] / x[-1]["ask"] > 0.0001
)  # filter on 0.1% spread as a per
flow.capture(StdOutputConfig())

if __name__ == "__main__":
    spawn_cluster(flow, **parse.cluster_args())
