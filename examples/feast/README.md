This example manufactures a Kafka pseudo-stream (bulk loaded in a batch) of driver data, partitions it into 24 hour epoch increments, averages driver `conv_rate` and `acc_rate` over each day and saves it to a Feast _online_ store using their alpha `write_to_online_store` operation.

The data is batched into the stream and then timestamped by the original event timestamps during intake.

To run locally:

From `feast` directory,

Bring up docker container with `docker-compose up`.
Run `pip install -r requirements.txt` to install dependencies.
Run `feast apply` to establish feature store/views.
Run `python utils/stream_data.py` to populate kafka stream.

if you run 
```
docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic drivers --from-beginning
```

you should see the "streaming" data in Kafka

Run `python feast_dataflow.py` to see data store in the online store.