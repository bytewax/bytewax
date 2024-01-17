Bytewax offers a lot of flexibility, and some choices can have a huge impact on performances.

In this article we explore ways to understand what's happening on a running dataflow.

## Metrics, Prometheus and Graphana
Bytewax dataflows can expose a webserver with a metrics endpoint that can be parsed by any Prometheus
compatible monitoring infrastructure.

To do that, you must run your dataflow with the env var `BYTEWAX_DATAFLOW_API_ENABLED` set to any value:

```bash
BYTEWAX_DATAFLOW_API_ENABLED=true python -m bytewax.run dataflow
```

Once the dataflow is running, and only until it keeps running, you can access the
metrics at `http://localhost:3030/metrics`.

You can configure Prometheus to read metrics from that endpoint and make queries on them.
A proper, production-ready setup for Prometheus and Grafana is out of the scope of this article,
Comment
but we can showcase a local development setup using docker-compose.

You'll need to create a couple of configuration files, one for Prometheus and one for Grafana.

First, create a file named `prometheus.yml` with the following content:

```yml
# prometheus.yml
global:
  scrape_interval: 10s
scrape_configs:
  - job_name: bytewax
    honor_timestamps: true
    metrics_path: /metrics
    scheme: http
    static_configs:
    - targets:
      - localhost:3030
```

This will instruct Prometheus to scrape the endpoint `http://localhost:3030/metrics` every 10 seconds.

Next, we can add a configuration file for Grafana, so we can preconfigure the Prometheus source in the Grafana instance.
Create a file named `datasource.yml`, we will add it to graphana in the docker compose file:

```yaml
# datasource.yml
apiVersion: 1

datasources:
- name: Prometheus
  type: prometheus
  url: http://localhost:9090
  isDefault: true
  access: proxy
  editable: true
```

This tells Grafana to look for a Prometheus source at `http://localhost:9090`.
The configurations used here assume everything is running on the same network.
In a real world scenario you'll have to use the proper urls.

Create a `docker-compose.yml` file with the two services and the needed configuration:

```yaml
# docker-compose.yml
version: "3.7"
services:
  prometheus:
    image: prom/prometheus:latest
    network_mode: "host"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
  grafana:
    image: grafana/grafana:latest
    network_mode: "host"
    volumes:
      - ./datasource.yml:/etc/grafana/provisioning/datasources/datasource.yaml
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=grafana
```

Run it:

```bash
docker compose up
```

If everything went right, you should be able to access Prometheus webui at `http://localhost:9090`.
From there you can see the metrics for your dataflow.
Grafana exposes its WebUI at `http://localhost:3000`.
You can login with the credentials provided in the docker-compose file,
and query your metrics and create dashboards there.
