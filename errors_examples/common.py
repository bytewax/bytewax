import sseclient
import urllib3

from bytewax.inputs import DynamicInput, StatelessSource


class WikiSource(StatelessSource):
    def __init__(self, client, events):
        self.client = client
        self.events = events

    def next(self):
        return next(self.events).data

    def close(self):
        self.client.close()


class WikiStreamInput(DynamicInput):
    def build(self, worker_index, worker_count):
        pool = urllib3.PoolManager()
        resp = pool.request(
            "GET",
            "https://stream.wikimedia.org/v2/stream/recentchange/",
            preload_content=False,
            headers={"Accept": "text/event-stream"},
        )
        client = sseclient.SSEClient(resp)

        return WikiSource(client, client.events())
