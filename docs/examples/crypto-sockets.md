
### Using Webhooks to Analyze Cryptocurrency Order Book in Real-Time and Profit
================================================

In this example we are going to walk through how you can maintain a limit order book in real-time with very little extra infrastructure with Bytewax.

We are going to:
- Use websockets to connect to an exchange (coinbase)
- Setup an order book using a snapshot
- Update the order book in real-time
- Use algorithms to trade crypto and profit. Just kidding, this is left to an exercise for the reader.

To start off, we are going to diverge into some concepts around markets, exchanges and orders.

A Limit Order Book, or just Order Book is a record of all limit orders that are made. a limit order is an order to buy (bid) or sell (ask) an asset for a given price. This could facilitate the exchange of dollars for shares or, as in our case, they could be orders to exchange crypto currencies. On exchanges, the limit order book is constantly changing as orders are placed every fraction of a second. The order book can give a trader insight into the market, whether they are looking to determine liquidity, to create liquidity, design a trading algorithm or maybe determine when bad actors are trying to manipulate the market. In the order book, the **ask price** is the lowest price that a seller is willing to sell at and the **bid price** is the highest price that a buyer is willing to buy at. A limit order is different than a market order in that the limit order can be placed with generally 4 dimensions, the direction (buy/sell), the size, the price and the duration (time to expire). A market order, in comparison, has 2 dimensions, the direction and the size. It is up to the exchange to fill a market order and it is filled via what is available in the order book.

An exchange will generally offer a few different tiers of information that traders can subscribe to. These can be quite expensive for some exchanges, but luckily for us, most crypto exchanges provide access to the various levels for free! For maintaining our order book we are going to need at least level2 order information. This gives us granularity of each limit order so that we can adjust the book in real-time. Without the ability to maintain our own order book, the snapshot would get almost instantly out of date and we would not be able to act with perfect information. We would have to download the order book every millisecond, or faster, to stay up to date and since the order book can be quite large, this isn't really feasible.

Alright, let's get started!

### Inputs & Outputs

We are going to use the `spawn_cluster` approach in this dataflow, this allows us to scale our dataflow in the future if we add additional products that we would like to track. To begin, we will need to build an input builder for our dataflow. In  the input builder we will receive each new message. To start, we use the coinbase pro websocket url (`wss://ws-feed.pro.coinbase.com`) and the Python websocket library to create a connection. Once connected we can send a message to the websocket subscribing to product_ids (pairs of currencies - USD/BTC for this example) and channels (level2 order book data). Finally since we know there will be some sort of acknowledgement message we can grab that with `ws.recv()` and print it out.

```Python
from websocket import create_connection
from bytewax import Dataflow, inputs, spawn_cluster

ws = create_connection("wss://ws-feed.pro.coinbase.com")
ws.send(
    json.dumps(
        {
            "type": "subscribe",
            "product_ids": ['BTC-USD'],
            "channels": ["level2"],
        }
    )
)
print(ws.recv())
```

At this point we are ready to create our input builder. The input builder is used to provide some control over how inputs are managed in the case that we have multiple workers. In this case we are only going to use one worker, but if we were going to have multiple workers here and multiple product_ids we would potentially want to control which were sent where so that they did not get exchanged later. Importantly, we decorate the input builder with `yield_epochs`. This decorator will handle the advancing of the epoch for us, this will prevent the dataflow from hanging on the last epoch when there is a pause or slow down in data throughput.

```Python
def ws_input():
    while True:
        yield (ws.recv())


@inputs.yield_epochs
def input_builder(worker_index, worker_count):
    return inputs.fully_ordered(ws_input())
```

Now that we have our input builder finished, we can create our output builder. The output builder is used in the `spawn_cluster` call that will kick-off our dataflow. The output builder is what will be called in the `capture` operator. For this example, we keep it simple, we are just going to print out the result of our dataflow to the council. 

```Python
def output_builder(worker_index, worker_count):
    return print
```

### Building Our Dataflow

Before we get to the exciting part of our order book dataflow we need to prep the data. To do this, we first load the json we are receiving from the websocket. Once loaded we can reformat the data to be a tuple of (product_id, data), which will permit us to aggregate by the product+id as our key in the next stop.

```Python
def key_on_product(data):
    return(data['product_id'],data)

flow = Dataflow()
flow.map(json.loads)
flow.map(key_on_product)
```

Now for the exciting part. The code below is what we are using to: 
1. Construct the orderbook as two dictionaries, one for asks, one for bids
2. Assign a value to the ask and bid price.
3. For each new order, update the order book and then update the bid and ask prices where required.
4. Return bid and ask price, the respective volumes of the ask and the difference between the prices.

So how does maintaining this `OrderBook` object work in a Dataflow. Below you will notice that we are using `stateful_map` as the operator for this aggregation step. `Stateful_map` will aggregate data based on a function (mapper), into an object that you define. The object must be defined via a builder, because it will create a new object via this builder for each new key received. The mapper must return the object so that it can be updated.


```Python
class OrderBook:
    def __init__(self):
        # if using Python < 3.7 need to use OrderedDict here
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

flow.stateful_map(lambda key: OrderBook(), OrderBook.update)
```

Finishing it up, for fun we can filter for a spread bigger than 5USD and then capture the output. Maybe we can profit off of this spread... or maybe not.

The `capture` operator is designed to use the output builder function that we defined earlier. In this case it will print out to our terminal.

```Python
flow.filter(lambda x: x[-1][-1] > 5.0)
flow.capture()
```

[Bytewax provides a few different entry points for executing your dataflow](/getting-started/execution/), in this example we are using `bytewax.spawn_cluster` which allows you to run dataflows in parallel on threads and processes.

```Python
spawn_cluster(flow, input_builder, output_builder)
```

That's it, let's verify our output:

```bash
{"type":"subscriptions","channels":[{"name":"level2","product_ids":["BTC-USD"]}]}
(1046, ('BTC-USD', (38590.1, 0.00945844, 38596.73, 0.01347429, 6.630000000004657)))
(1047, ('BTC-USD', (38590.1, 0.00945844, 38597.13, 0.02591147, 7.029999999998836)))
(1048, ('BTC-USD', (38590.1, 0.00945844, 38597.13, 0.02591147, 7.029999999998836)))
```

That's it!

