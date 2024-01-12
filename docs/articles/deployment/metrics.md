Bytewax offers a lot of flexibility, and some choices can have a huge impact on performances.

In this article we explore ways to understand what's happening on a running dataflow.

## Metrics, Prometheus and Graphana
Bytewax dataflows can expose a webserver with a metrics endpoint that can be parsed by Prometheus.

Run your dataflow with the env var `BYTEWAX_DATAFLOW_API_ENABLED` set to "true":

```bash
BYTEWAX_DATAFLOW_API_ENABLED=true python -m bytewax.run dataflow
```

Now you can access the metrics page at `http://localhost:3030/metrics`
You can use Prometheus to read metrics from that endpoint and make queries on them.

A proper production setup for Prometheus + Graphana stack is out of the scope of this article,
but we can showcase a local development setup using docker-compose.

You first need to create a minimal configuration for both prometheus and graphana.
Add a file named `prometheus.yml` with the following content:

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

And a file named `datasource.yml` to preconfigure our prometheus instance to work with graphana:

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

Then run prometheus and graphana with the following `docker-compose.yml`:

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

Run:

```bash
docker compose up
```

Now you can access `http://localhost:9090` and see the metrics for your dataflow in the prometheus
web UI, or go to `http://localhost:3000` and work with your metrics in the graphana ui.
