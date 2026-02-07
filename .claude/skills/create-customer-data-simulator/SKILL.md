---
name: This runs the datasurface customer data simulator
description: This runs a simulator that generates data for customers and their addresses for testing purposes.
---

# Create source tables and initial test data using the data simulator

The simulator uses a database called customer_db. The simulator itself will create the 2 tables (customers, addresses) and then start populating them with data and simulating changes. It supports PostgreSQL, SQL Server, and Snowflake databases.

## Environment Variables

Ask the user which database type they want to target, then collect the appropriate variables.

### Common variables

```bash
NAMESPACE=demo1                  # Kubernetes namespace
DATASURFACE_VERSION=1.1.0        # DataSurface image version
DB_TYPE=postgres                 # One of: postgres, sqlserver, snowflake
DB_HOST=host.docker.internal     # Database hostname
DB_DATABASE=customer_db          # Database name
```

### PostgreSQL

```bash
DB_TYPE=postgres
DB_USER=postgres
DB_PASSWORD=password
DB_HOST=host.docker.internal     # Docker Desktop: host.docker.internal
DB_PORT=5432                     # Default for postgres
```

### SQL Server

```bash
DB_TYPE=sqlserver
DB_USER=sa
DB_PASSWORD='pass@w0rd'
DB_HOST=sqlserver-co             # Remote SQL Server hostname
DB_PORT=1433                     # Default for sqlserver
```

### Snowflake

```bash
DB_TYPE=snowflake
DB_USER=DATASURFACE
DB_ACCOUNT=HORSEQD-GQC71098     # Snowflake account identifier
DB_DATABASE=SNOWFLAKE_LEARNING_DB
DB_WAREHOUSE=COMPUTE_WH
DB_SCHEMA=PUBLIC                 # Default: PUBLIC
DB_ROLE=                         # Optional Snowflake role
```

Snowflake uses key-pair authentication by default. The private key is resolved in order:
1. `--private-key-path <file>` — explicit PEM file path
2. `SNOWFLAKE_PRIVATE_KEY` env var — for K8s pods (secret injected as env var)
3. `~/.snowflake/rsa_key.p8` — default local file

If none are found, falls back to `--user`/`--password`.

## Step 1: Create the customer_db database (if it doesn't exist)

### PostgreSQL (Docker Desktop)

```bash
docker exec datasurface-postgres psql -U $DB_USER -lqt | grep customer_db
# If it doesn't exist:
docker exec datasurface-postgres psql -U $DB_USER -c "CREATE DATABASE customer_db;"
```

### PostgreSQL (remote)

```bash
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -lqt | grep customer_db
# If it doesn't exist:
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -c "CREATE DATABASE customer_db;"
```

### SQL Server

```bash
sqlcmd -S $DB_HOST -U $DB_USER -P $DB_PASSWORD -C -Q "SELECT name FROM sys.databases WHERE name = 'customer_db'"
# If it doesn't exist:
sqlcmd -S $DB_HOST -U $DB_USER -P $DB_PASSWORD -C -Q "CREATE DATABASE customer_db"
```

### Snowflake

The simulator creates the database and tables automatically with `--create-tables`.

## Step 2: Test locally with Docker (recommended before K8s deployment)

Run a quick test with `--max-changes 10` to verify connectivity before deploying to the cluster.

**Important:** If the database host is a Tailscale or non-public hostname, Docker containers cannot resolve it directly. Use `--add-host` to map the hostname to its IP:

```bash
# Resolve the hostname first
dscacheutil -q host -a name $DB_HOST
```

### PostgreSQL

```bash
docker run --rm \
  registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  python src/tests/data_change_simulator.py \
  --db-type postgres \
  --host "$DB_HOST" \
  --port "$DB_PORT" \
  --database customer_db \
  --user "$DB_USER" \
  --password "$DB_PASSWORD" \
  --create-tables \
  --max-changes 10 \
  --verbose
```

### SQL Server

```bash
docker run --rm \
  --add-host=$DB_HOST:<IP_ADDRESS> \
  registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  python src/tests/data_change_simulator.py \
  --db-type sqlserver \
  --host "$DB_HOST" \
  --port "$DB_PORT" \
  --database customer_db \
  --user "$DB_USER" \
  --password "$DB_PASSWORD" \
  --create-tables \
  --max-changes 10 \
  --verbose
```

### Snowflake

```bash
docker run --rm \
  registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  python src/tests/data_change_simulator.py \
  --db-type snowflake \
  --account "$DB_ACCOUNT" \
  --database "$DB_DATABASE" \
  --user "$DB_USER" \
  --warehouse "$DB_WAREHOUSE" \
  --create-tables \
  --max-changes 10 \
  --verbose
```

## Step 3: Check for existing simulator pod

```bash
kubectl get pod data-simulator -n $NAMESPACE
```

If a pod already exists, delete it first:

```bash
kubectl delete pod data-simulator -n $NAMESPACE
```

## Step 4: Start the data simulator on Kubernetes

### PostgreSQL

```bash
kubectl run data-simulator --restart=Never \
  --image=registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --db-type postgres \
  --host "$DB_HOST" \
  --port "$DB_PORT" \
  --database customer_db \
  --user "$DB_USER" \
  --password "$DB_PASSWORD" \
  --create-tables \
  --max-changes 1000000 \
  --verbose
```

### SQL Server

```bash
kubectl run data-simulator --restart=Never \
  --image=registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --db-type sqlserver \
  --host "$DB_HOST" \
  --port "$DB_PORT" \
  --database customer_db \
  --user "$DB_USER" \
  --password "$DB_PASSWORD" \
  --create-tables \
  --max-changes 1000000 \
  --verbose
```

### Snowflake

For K8s, inject the private key via a secret as an environment variable:

```bash
kubectl run data-simulator --restart=Never \
  --image=registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  --env="SNOWFLAKE_PRIVATE_KEY=$(kubectl get secret snowflake-key -n $NAMESPACE -o jsonpath='{.data.private-key}' | base64 -d)" \
  --env="SNOWFLAKE_PASSPHRASE=$(kubectl get secret snowflake-key -n $NAMESPACE -o jsonpath='{.data.passphrase}' | base64 -d)" \
  -n "$NAMESPACE" \
  -- python src/tests/data_change_simulator.py \
  --db-type snowflake \
  --account "$DB_ACCOUNT" \
  --database "$DB_DATABASE" \
  --user "$DB_USER" \
  --warehouse "$DB_WAREHOUSE" \
  --create-tables \
  --max-changes 1000000 \
  --verbose
```

**Note:** When running on a remote cluster where the database hostname requires DNS configuration (e.g., Tailscale hostnames), ensure CoreDNS is configured with the appropriate host entries. See the **remote-setup-walkthrough** skill for CoreDNS configuration.

## Step 5: Verify the simulator is running

Wait a moment for the simulator to start, then check status and logs:

```bash
kubectl get pod data-simulator -n $NAMESPACE
kubectl logs data-simulator -n $NAMESPACE --tail=20
```

You should see messages like:
- "Connected to SQLServerDatabase(...)" or "Connected to PostgresDatabase(...)"
- "CREATED customer..." or "UPDATED customer..."
- "ADDED address..." or "DELETED address..."

## Stopping the simulator

```bash
kubectl delete pod data-simulator -n $NAMESPACE
```

## Simulator operations

The simulator performs these operations with weighted random selection:

| Operation | Weight | Description |
|-----------|--------|-------------|
| INSERT customer | 15% | New customer with initial address |
| UPDATE customer | 25% | Change email and/or phone |
| INSERT address | 20% | Add address to existing customer |
| UPDATE address | 30% | Change street, city, state, or zip |
| DELETE address | 10% | Remove an address |

## Troubleshooting

### Login timeout (SQL Server / PostgreSQL)

- Verify the database host is reachable from inside pods: `kubectl run test --rm -it --restart=Never --image=busybox -n $NAMESPACE -- nc -zv $DB_HOST $DB_PORT`
- For remote clusters, check CoreDNS has entries for the database hostname
- For Docker Desktop, ensure `DB_HOST=host.docker.internal`

### Snowflake authentication failures

- Verify the private key is correctly injected: `kubectl exec data-simulator -n $NAMESPACE -- env | grep SNOWFLAKE`
- Check that the Snowflake user has the correct RSA public key registered
- Try with `--password` flag to fall back to password auth for debugging
