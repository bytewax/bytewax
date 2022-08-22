Analyzing Cryptocurrency Order Book in Real-Time
================================================

In this example we are going to walk through how you can maintain a limit order book in real-time with very little extra infrastructure with Bytewax.

We are going to:
* Use websockets to connect to an exchange (coinbase)
* Setup an order book using a snapshot
* Update the order book in real-time
* Use algorithms to trade crypto and profit. Just kidding, this is left to an exercise for the reader.

To start off, we are going to diverge into some concepts around markets, exchanges and orders.

Concepts
--------

**Order Book**
__________

A Limit Order Book, or just Order Book is a record of all limit orders that are made. A limit order is an order to buy (bid) or sell (ask) an asset for a given price. This could facilitate the exchange of dollars for shares or, as in our case, they could be orders to exchange crypto currencies. On exchanges, the limit order book is constantly changing as orders are placed every fraction of a second. The order book can give a trader insight into the market, whether they are looking to determine liquidity, to create liquidity, design a trading algorithm or maybe determine when bad actors are trying to manipulate the market.

**Bid and Ask**
_______________
In the order book, the **ask price** is the lowest price that a seller is willing to sell at and the **bid price** is the highest price that a buyer is willing to buy at. A limit order is different than a market order in that the limit order can be placed with generally 4 dimensions, the direction (buy/sell), the size, the price and the duration (time to expire). A market order, in comparison, has 2 dimensions, the direction and the size. It is up to the exchange to fill a market order and it is filled via what is available in the order book.

**Level 2 Data**
________________
An exchange will generally offer a few different tiers of information that traders can subscribe to. These can be quite expensive for some exchanges, but luckily for us, most crypto exchanges provide access to the various levels for free! For maintaining our order book we are going to need at least level2 order information. This gives us granularity of each limit order so that we can adjust the book in real-time. Without the ability to maintain our own order book, the snapshot would get almost instantly out of date and we would not be able to act with perfect information. We would have to download the order book every millisecond, or faster, to stay up to date and since the order book can be quite large, this isn't really feasible.

Alright, let's get started!

Inputs & Outputs
----------------

We are going to eventually create a cluster of dataflows where we could have multiple currency pairs running in parallel on different workers. In order to follow this approach, we will use the [`spawn_cluster`](https://docs.bytewax.io/apidocs#bytewax.spawn_cluster) method of kicking off our dataflow. To start, we will build a websocket input function that will use the coinbase pro websocket url (`wss://ws-feed.pro.coinbase.com`) and the Python websocket library to create a connection. Once connected we can send a message to the websocket subscribing to product_ids (pairs of currencies - USD-BTC for this example) and channels (level2 order book data). Finally since we know there will be some sort of acknowledgement message we can grab that with `ws.recv()` and print it out.

```python
import json
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig

PRODUCT_IDS = ['BTC-USD','ETH-USD']
```

```python doctest:SKIP
from websocket import create_connection

def ws_input(product_ids):
    state = None
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
        yield state, ws.recv()
```

For testing purposes, if you don't want to perform HTTP requests, you can read the sample websocket data from a local file with a list of JSON responses.

```python
def ws_input(product_ids, state):
    print('{"type":"subscriptions","channels":[{"name":"level2","product_ids":["BTC-USD","ETH-USD"]}]}')
    for msg in open("crypto-sockets.json"):
        yield state, msg
```

Now that we have our websocket based data generator built, we will write an input builder for our dataflow. Since we're using a manual input builder, we'll pass a `ManualInputConfig` as our `input_config` with the builder as a parameter. The input builder is called on each worker and the function will have information about the `worker_index` and the total number of workers (`worker_count`). In this case we are designing our input builder to handle multiple workers and multiple currency pairs, so that we can parallelize the input. So we will distribute the currency pairs with the logic in the code below. It should be noted that if you run more than one worker with only one currency pair, the other workers will not be used.

```python
from bytewax.inputs import ManualInputConfig

def input_builder(worker_index, worker_count, resume_state):
    # We're not going to worry about stateful recovery in this example.
    state = None
    prods_per_worker = int(len(PRODUCT_IDS)/worker_count)
    product_ids = PRODUCT_IDS[int(worker_index*prods_per_worker):int(worker_index*prods_per_worker+prods_per_worker)]
    return ws_input(product_ids, state)

input_config = ManualInputConfig(input_builder)
```

Now that we have our input builder finished, we can create our output builder. The output builder is used in conjunction with `spawn_cluster`. In a dataflow, when you use the `capture` operator, the output_builder is called and will receive information about the worker and the data. For this example, we keep it simple, we are just going to print out the result of our dataflow to the terminal.

```python
def output_builder(worker_index, worker_count):
    return print
```

Building Our Dataflow
---------------------

Before we get to the exciting part of our order book dataflow we need to prep the data. We initially receive some JSON formatted text, so we will first deserialize the JSON we are receiving from the websocket into a dictionary. Once deserialized, we can reformat the data to be a tuple of the shape (product_id, data). This will permit us to aggregate by the product_id as our key in the next step.

```python
from bytewax.dataflow import Dataflow

def key_on_product(data):
    return(data['product_id'],data)

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(json.loads)
# {'type': 'l2update', 'product_id': 'BTC-USD', 'changes': [['buy', '36905.39', '0.00334873']], 'time': '2022-05-05T17:25:09.072519Z'}
flow.map(key_on_product)
# ('BTC-USD', {'type': 'l2update', 'product_id': 'BTC-USD', 'changes': [['buy', '36905.39', '0.00334873']], 'time': '2022-05-05T17:25:09.072519Z'})
```

Now for the exciting part. The code below is what we are using to:
1. Construct the orderbook as two dictionaries, one for asks, one for bids
2. Assign a value to the ask and bid price.
3. For each new order, update the order book and then update the bid and ask prices where required.
4. Return bid and ask price, the respective volumes of the ask and the difference between the prices.

The data from the coinbase pro websocket is first a snapshot of the current order book in the format:

```json
{
  "type": "snapshot",
  "product_id": "BTC-USD",
  "bids": [["10101.10", "0.45054140"]...],
  "asks": [["10102.55", "0.57753524"]...]
}
```

  and then each additional message is an update with a new limit order of the format:

```json
{
  "type": "l2update",
  "product_id": "BTC-USD",
  "time": "2019-08-14T20:42:27.265Z",
  "changes": [
    [
      "buy",
      "10101.80000000",
      "0.162567"
    ]
  ]
}
```

To maintain an order book in real time, we will first need to construct an object to hold the orders from the snapshot and then update that object with each additional update. This is a good use case for the [`stateful_map`](https://docs.bytewax.io/apidocs#bytewax.Dataflow.stateful_map) operator, which can aggregate by key. `Stateful_map` will aggregate data based on a function (mapper), into an object that you define. The object must be defined via a builder, because it will create a new object via this builder for each new key received. The mapper must return the object so that it can be updated.

Below we have the code for the OrderBook object that has a bids and asks dictionary. These will be used to first create the order book from the snapshot and once created we can attain the first bid price and ask price. The bid price is the highest buy order placed and the ask price is the lowest sell order places. Once we have determined the bid and ask prices, we will be able to calculate the spread and track that as well.

```python
class OrderBook:
    def __init__(self):
        # if using Python < 3.7 need to use OrderedDict here
        self.bids = {}
        self.asks = {}

    def update(self, data):
        if self.bids == {}:
            self.bids = {float(price):float(size) for price, size in data['bids']}
            # The bid_price is the highest priced buy limit order.
            # since the bids are in order, the first item of our newly constructed bids
            # will have our bid price, so we can track the best bid
            self.bid_price = next(iter(self.bids))
        if self.asks == {}:
            self.asks = {float(price):float(size) for price, size in data['asks']}
            # The ask price is the lowest priced sell limit order.
            # since the asks are in order, the first item of our newly constructed
            # asks will be our ask price, so we can track the best ask
            self.ask_price = next(iter(self.asks))
        else:
            # We receive a list of lists here, normally it is only one change,
            # but could be more than one.
            for update in data['changes']:
                price = float(update[1])
                size = float(update[2])
            if update[0] == 'sell':
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
            if update[0] == 'buy':
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
        return self, {'bid': self.bid_price, 'bid_size': self.bids[self.bid_price], 'ask': self.ask_price, 'ask_price': self.asks[self.ask_price], 'spread': self.ask_price-self.bid_price}

flow.stateful_map("order_book", OrderBook, OrderBook.update)
```

With our snapshot processed, for each new message we receive from the websocket, we can update the order book, the bid and ask price and the spread via the `update` method. Sometimes an order was filled or it was cancelled and in this case what we receive from the update is something similar to `'changes': [['buy', '36905.39', '0.00000000']]`. When we receive these updates of size `'0.00000000'`, we can remove that item from our book and potentially update our bid and ask price. The code below will check if the order should be removed and if not it will update the order. If the order was removed, it will check to make sure the bid and ask prices are modified if required.

Finishing it up, for fun we can filter for a spread as a percentage of the ask price greater than 01% and then capture the output. Maybe we can profit off of this spread... or maybe not.

The `capture` operator is designed to use the output builder function that we defined earlier. In this case it will print out to our terminal.

```python
flow.filter(lambda x: x[-1]['spread'] / x[-1]['ask'] > 0.0001)
flow.capture(ManualOutputConfig(output_builder))
```

[Bytewax provides a few different entry points for executing a dataflow](/getting-started/execution/).
That's it, let's run it and verify our output:

```python
from bytewax.execution import run_main

run_main(flow)
```

```{testoutput}
{"type":"subscriptions","channels":[{"name":"level2","product_ids":["BTC-USD","ETH-USD"]}]}
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.05, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.05, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.05, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.05, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.05, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.04939131, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.04939131, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('BTC-USD', {'bid': 19814.24, 'bid_size': 0.04939131, 'ask': 19816.94, 'ask_price': 0.5038223, 'spread': 2.6999999999970896})
('ETH-USD', {'bid': 1088.41, 'bid_size': 7.6889646, 'ask': 1088.7, 'ask_price': 0.12011129, 'spread': 0.2899999999999636})
```

Another entry point `bytewax.spawn_cluster` which allows us to run dataflows in parallel on threads and processes.

```python doctest:SKIP
from bytewax import parse, spawn_cluster

spawn_cluster(flow, input_config, output_builder, **parse.cluster_args())
```

If running from the command line, you can invoke it yourself with:

```bash
$ python orderbook.py
# for multiple workers --> python orderbook.py -w 2
```

And eventually, if the spread is greater than $5, we will see some output similar to what is below.

```bash
{"type":"subscriptions","channels":[{"name":"level2","product_ids":["BTC-USD"]}]}
(1046, ('BTC-USD', (38590.1, 0.00945844, 38596.73, 0.01347429, 6.630000000004657)))
(1047, ('BTC-USD', (38590.1, 0.00945844, 38597.13, 0.02591147, 7.029999999998836)))
(1048, ('BTC-USD', (38590.1, 0.00945844, 38597.13, 0.02591147, 7.029999999998836)))
```

That's it!

We would love to see if you can build on this example. Feel free to share what you've built in our [community slack channel](https://join.slack.com/t/bytewaxcommunity/shared_invite/zt-vkos2f6r-_SeT9pF2~n9ArOaeI3ND2w).
