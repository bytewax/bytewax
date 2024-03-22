# Pub/Sub and Polling

For building resilient systems, best practices suggest a separation
between different components of the infrastructure. Databases, queues,
and logs play a critical role in this separation, bridging the gap
between parts of the system that generate data (events, records, etc.)
and those that consume it. In streaming systems, queues and logs are
particularly prominent. Popular platforms in this domain include
Kafka, RabbitMQ, SQS, NATS, and Redpanda.

In this section of the Bytewax documentation, we unpack various
delivery semantics commonly encountered in data systems, specifically
streaming systems. Understanding these concepts is crucial for
designing efficient and reliable data processing systems. We'll cover
some of the more common patterns encountered and briefly explain how
they can be accomplished or not with Bytewax.

The most commonly used patterns with data infrastructure are listed
below:

* Request/Response
* Request/ACK
* Request/Forget
* PubSub
* Mailbox
* Polling

## Request/Response

The Request/Response pattern is a synchronous communication method
where a request is sent to a service, and the sender waits for a
response. This pattern is typical in RESTful APIs and database
queries. It is not common in stream processing systems.

## Acknowledgment

Request/ACK extends the basic request/response pattern by adding an
acknowledgment step. The receiver sends an ACK message back to the
sender to confirm receipt. This pattern is more common in distributed
systems and acknowledgment patterns are often used with queues like
AWS SQS and RabbitMQ. The consumer of the message will acknowledge the
successful processing. For exactly-once guarantees the acknowledgement
needs to be coordinated with the successful delivery to the final part
of the system. For example, the service would acknowledge successful
processing once it has written the data to a database after finishing
processing.

## Request/Forget

Request/Forget is an asynchronous pattern where the sender emits a
request and immediately moves on without waiting for a response or
acknowledgment. It's useful for logging, metrics collection, or any
scenario where the sender doesn't need confirmation of receipt or
processing. Bytewax can fulfill this pattern when coupled with a
system that can decouple the request effectively from the processing.
One example would be to use Redpanda or Kafka with an HTTP proxy and
Bytewax consuming from the Kafka/Redpanda topic.

## PubSub (Publish/Subscribe)

PubSub is a messaging paradigm where messages are published to topics
and received by subscribers. It's inherently asynchronous and supports
one-to-many communication. Ideal for broadcasting data, like stock
price updates, where multiple consumers might be interested in the
same data. Kafka is one of the most common pubsub systems that can be
used with Bytewax.

## Mailbox

The Mailbox pattern is similar to an email system. Messages are sent
to a mailbox where they are stored until the recipient retrieves them.
NATS provides a convenient cloud native Queue with mailbox semantics
that can be coupled with a stream processor to process the message.
This pattern is useful in scenarios where message ordering is crucial,
or when the recipient processes messages at their own pace, like task
queues for asynchronous job processing. A mailbox pattern can also be
used in a request/response pattern when the publisher of the message
to the mailbox waits for the mailbox to receive a new message when
processing is complete.

## Polling

Polling involves regularly querying or checking a source for new data
or updates. It's a simple but effective way to retrieve data from
sources that don't support push mechanisms. Polling is common in
scenarios where the data source doesn't support real-time updates,
like checking for updates from a third-party API. Since many systems
are not designed for push capabilities, under the hood, polling is
often used in a very eager manner. An example of this is Kafka
consumers. Kafka consumers poll kafka eagerly for new data giving the
appearance of a push-based system.
