import json, time

from websocket import create_connection
from bytewax import Dataflow, inputs, spawn_cluster

PRODUCT_IDS = ['BTC-USD','ETH-USD']

@inputs.yield_epochs
def input_builder(worker_index, worker_count):
    prods_per_worker = int(len(PRODUCT_IDS)/worker_count)
    product_ids = PRODUCT_IDS[int(worker_index*prods_per_worker):int(worker_index*prods_per_worker+prods_per_worker)]
    return inputs.fully_ordered(ws_input(product_ids))

def ws_input(product_ids):
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
    print(ws.recv())
    while True:
        yield (ws.recv())


def output_builder(worker_index, worker_count):
    return print

def key_on_product(data):
    return(data['product_id'],data)

class OrderBook:
    def __init__(self):
        # if using Python <3.7 need to use OrderedDict here
        self.bids = {}
        self.asks = {}

    def update(self, data):
        if self.bids == {}:
            self.bids = {float(v[0]):float(v[1]) for v in data['bids']}
            self.bid_price = next(iter(self.bids))
        if self.asks == {}:
            self.asks = {float(v[0]):float(v[1]) for v in data['asks']}
            self.ask_price = next(iter(self.asks))
        else:
            for update in data['changes']:
                price = float(update[1])
                size = float(update[2])
            if update[0] == 'sell':
                # modify asks
                if size == 0.0:
                    try:
                        del self.asks[price]
                        if price <= self.ask_price:
                            self.ask_price = sorted(self.asks.keys())[0]
                    except KeyError:
                        # don't need to add price with size zero
                        pass
                else:
                    self.asks[price] = size
                    if price < self.ask_price:
                        self.ask_price = price
            if update[0] == 'buy':
                # modify bids
                if size == 0.0:
                    try:
                        del self.bids[price]
                        if price >= self.bid_price:
                            self.bid_price = sorted(self.bids.keys())[-1]
                    except KeyError:
                        # don't need to add price with size zero
                        pass
                else:
                    self.bids[price] = size
                    if price > self.bid_price:
                        self.bid_price = price
        return self, (self.bid_price, self.bids[self.bid_price], self.ask_price, self.asks[self.ask_price], self.ask_price-self.bid_price)


flow = Dataflow()
flow.map(json.loads)
# {'type': 'l2update', 'product_id': 'BTC-USD', 'changes': [['buy', '36905.39', '0.00334873']], 'time': '2022-05-05T17:25:09.072519Z'}
flow.map(key_on_product)
# ('BTC-USD', {'type': 'l2update', 'product_id': 'BTC-USD', 'changes': [['buy', '36905.39', '0.00334873']], 'time': '2022-05-05T17:25:09.072519Z'})
flow.stateful_map(lambda key: OrderBook(), OrderBook.update)
# ('BTC-USD', (36905.39, 0.00334873, 36905.4, 1.6e-05, 0.010000000002037268))
# flow.filter(lambda x: x[-1][-1] > 5.0)
flow.capture()

if __name__ == "__main__":
    spawn_cluster(flow, input_builder, output_builder)
