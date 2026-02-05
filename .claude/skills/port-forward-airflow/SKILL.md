---
name: Port Forward Airflow
description: Port forward the Airflow 3.x API server to access the web UI locally.
---
# Port Forward Airflow API Server

Port forward the Airflow 3.x API server from your Kubernetes cluster to access the web UI on localhost.

## Usage

```bash
kubectl port-forward -n <namespace> svc/airflow-api-server 8080:8080
```

Then open `http://localhost:8080` in your browser.

## Alternative Local Port

If port 8080 is already in use, specify a different local port:

```bash
kubectl port-forward -n <namespace> svc/airflow-api-server 9090:8080
```

Access at `http://localhost:9090`

## Note

Airflow 3.x renamed the `webserver` component to `api-server`. If you're looking for `airflow-webserver`, use `airflow-api-server` instead.
