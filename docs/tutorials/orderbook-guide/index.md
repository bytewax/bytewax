# Real-Time Financial Exchange Order Book

![Currency symbol](thumbnail.svg)


In this example we are going to walk through how you can maintain a limit order book in real-time with very little extra infrastructure with Bytewax.


| Skill Level | Time to Complete | Level |
| ----------- | ---------------- | ----- |
| Intermediate Python programming, asynchronous programming | Approx. 25 Min | Intermediate |


## Your Takeaway

At the end of this tutorial you will understand how to use Bytewax to analyze financial exchange data. You will learn to establish connections to a WebSocket for real-time data, use Bytewax's operators to efficiently manage an order book, and apply analytical techniques to assess trading opportunities based on the dynamics of buy and sell orders.

## Resources

<gh-path:/docs/tutorials/orderbook-guide/orderbook_dataflow.py>

## Objectives

In this example we are going to walk through how you can maintain a limit order book in real-time with very little extra infrastructure with Bytewax.

We are going to:

* Connect to Coinbase via WebSockets for live order book updates.
* Initialize order books with current snapshots for major cryptocurrencies.
* Update order books in real-time with market changes.
* Utilize advanced data structures for efficient order book management.
* Process live data with Bytewax to maintain and summarize order books.
* Filter updates for significant market movements based on spread.

## Concepts

To start off, we are going to diverge into some concepts around markets, exchanges and orders.

### Order Book

A Limit Order Book, or just Order Book is a record of all limit orders that are made. A limit order is an order to buy (bid) or sell (ask) an asset for a given price. This could facilitate the exchange of dollars for shares or, as in our case, they could be orders to exchange crypto currencies. On exchanges, the limit order book is constantly changing as orders are placed every fraction of a second. The order book can give a trader insight into the market, whether they are looking to determine liquidity, to create liquidity, design a trading algorithm or maybe determine when bad actors are trying to manipulate the market.

### Bid and Ask

In the order book, the ask price is the lowest price that a seller is willing to sell at and the bid price is the highest price that a buyer is willing to buy at. A limit order is different than a market order in that the limit order can be placed with generally 4 dimensions, the direction (buy/sell), the size, the price and the duration (time to expire). A market order, in comparison, has 2 dimensions, the direction and the size. It is up to the exchange to fill a market order and it is filled via what is available in the order book.

### Level 2 Data

An exchange will generally offer a few different tiers of information that traders can subscribe to. These can be quite expensive for some exchanges, but luckily for us, most crypto exchanges provide access to the various levels for free! For maintaining our order book we are going to need at least level2 order information. This gives us granularity of each limit order so that we can adjust the book in real-time. Without the ability to maintain our own order book, the snapshot would get almost instantly out of date and we would not be able to act with perfect information. We would have to download the order book every millisecond, or faster, to stay up to date and since the order book can be quite large, this isn't really feasible.

Alright, let's get started!

## Set up and imports

Before we begin, let's import the necessary modules and set up the environment for building the dataflow.

Complete installation - we recommend using a virtual environment to manage your Python dependencies. You can install Bytewax using pip:

```{code-block} console
:substitutions:
$ python -m venv venv
$ ./venv/bin/activate
(venv) $ pip install bytewax==|version|
```

Now, let's import the required modules and set up the environment for building the dataflow.

```{literalinclude} orderbook_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-imports
:end-before: end-imports
:lineno-match:
```

## Websocket Input
Our goal is to build a scalable system that can monitor multiple cryptocurrency pairs across different workers in real time. By crafting an asynchronous function to connect to the Coinbase Pro WebSocket, we facilitate streaming of cryptocurrency data into our dataflow. This process involves the websockets Python library for WebSocket connections and bytewax for dataflow integration.

The function `_ws_agen` inputs cryptocurrency pair identifiers (e.g., `["BTC-USD", "ETH-USD"]`), establishing a connection to Coinbase Pro's WebSocket. It subscribes to the `level2_batch` channel for live order book updates, sending a JSON subscription message and awaiting a confirmation response with `ws.recv()`.

```{literalinclude} orderbook_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-async
:end-before: end-async
:lineno-match:
```
