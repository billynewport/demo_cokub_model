---
name: This runs the datasurface customer data simulator
description: This runs a simulator that generates data for customers and their addresses for testing purposes.
---

# Create source tables and initial test data using the data simulator

The simulator uses a database called customer_db. The simulator itself will create the 2 tables and then start populating them with data and simulating changes.

## Environment Variables

Refer to the **setup-walkthrough** skill for full environment configuration details. The default values are:

```bash
NAMESPACE=demo1
DATASURFACE_VERSION=1.1.0
PG_USER=postgres
PG_PASSWORD=password
PG_HOST=host.docker.internal  # Required for K8s pods to reach Docker host
PG_PORT=5432
```

**Note:** `PG_HOST` must be `host.docker.internal` when running in Kubernetes on Docker Desktop, since PostgreSQL runs as a Docker container on the host machine.

## Step 1: Create the customer_db database (if it doesn't exist)

Check if the database exists:

```bash
docker exec datasurface-postgres psql -U postgres -lqt | grep customer_db
```

If it doesn't exist, create it:

```bash
docker exec datasurface-postgres psql -U postgres -c "CREATE DATABASE customer_db;"
```

## Step 2: Check for existing simulator pod

```bash
kubectl get pod data-simulator -n $NAMESPACE
```

If a pod already exists, delete it first:

```bash
kubectl delete pod data-simulator -n $NAMESPACE
```

## Step 3: Start the data simulator

This creates the customers and addresses tables with initial data and simulates changes continuously:

```bash
kubectl run data-simulator --restart=Never \
  --image=registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  --env="POSTGRES_USER=$PG_USER" \
  --env="POSTGRES_PASSWORD=$PG_PASSWORD" \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --host "$PG_HOST" \
  --port "$PG_PORT" \
  --database customer_db \
  --user "$PG_USER" \
  --password "$PG_PASSWORD" \
  --create-tables \
  --max-changes 1000000 \
  --verbose
```

## Step 4: Verify the simulator is running

Wait a moment for the simulator to start, then check status and logs:

```bash
kubectl get pod data-simulator -n $NAMESPACE
kubectl logs data-simulator -n $NAMESPACE --tail=20
```

You should see messages like:
- "Connected to database customer_db"
- "Tables are ready!"
- "CREATED customer..." or "UPDATED customer..."

## Stopping the simulator

```bash
kubectl delete pod data-simulator -n $NAMESPACE
```
