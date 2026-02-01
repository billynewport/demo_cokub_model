---
name: Generate DataSurface Yellow Bootstrap Artifacts
description: Generate Kubernetes manifests, Airflow DAGs, and initialization jobs for Yellow deployment.
---
# Generating Yellow Bootstrap Artifacts

Bootstrap artifacts are the Kubernetes manifests and Airflow DAGs needed to deploy and operate a DataSurface Yellow environment.

## Prerequisites

1. **DataSurface Docker image pulled:**

```bash
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
```

1. **Model configured** with correct repository settings in `eco.py`:

```python
GIT_REPO_OWNER: str = "yourorg"
GIT_REPO_NAME: str = "demo1_actual"
```

1. **Runtime environment defined** in `rte_demo.py` with proper credentials and database configuration.

## Ring Levels Explained

Yellow uses a "ring" system for staged deployment:

| Ring Level | Purpose | When to Run | Artifacts Generated |
| ------------ | --------- | ------------- | --------------------- |
| **0** | Initial bootstrap | Once, before K8s setup | kubernetes-bootstrap.yaml, infrastructure DAG, init job, merge job |
| **1** | Schema initialization | After K8s bootstrap applied | Creates database schemas, tables, initializes merge DB |

**Ring 0** generates the files you need to set up Kubernetes.
**Ring 1** runs inside Kubernetes to initialize the database schemas.

## Generating Ring 0 Artifacts (Local)

Run this from your model directory:

```bash
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Demo_PSP \
  --rte-name demo
```

### Parameters

| Parameter | Description | Example |
| ----------- | ------------- | --------- |
| `--ringLevel` | Bootstrap stage (0 or 1) | `0` |
| `--model` | Path to model directory (inside container) | `/workspace/model` |
| `--output` | Output directory for generated files | `/workspace/model/generated_output` |
| `--psp` | Platform Service Provider name from model | `Demo_PSP` |
| `--rte-name` | Runtime Environment name from model | `demo` |

## Generated Artifacts (Ring 0)

After running Ring 0 generation, you'll have:

```text
generated_output/
└── Demo_PSP/
    ├── kubernetes-bootstrap.yaml      # PVC, ConfigMap, NetworkPolicy, MCP Server
    ├── demo_psp_infrastructure_dag.py # Airflow DAG for ongoing operations
    ├── demo_psp_ring1_init_job.yaml   # K8s Job for Ring 1 initialization
    ├── demo_psp_model_merge_job.yaml  # K8s Job for model merge operations
    └── demo_psp_reconcile_views_job.yaml  # K8s Job for view reconciliation
```

### Artifact Descriptions

**kubernetes-bootstrap.yaml** contains:

- `git-cache-pvc` - PersistentVolumeClaim for caching git repository
- `demo-logging-config` - ConfigMap for logging configuration
- `demo-network-policy` - NetworkPolicy for pod communication
- `demo-psp-mcp-server` - Deployment for Model Context Protocol server
- `demo-psp-mcp` - Service exposing MCP server

**demo_psp_infrastructure_dag.py** - Airflow DAG that:

- Monitors model repository for changes
- Triggers merge jobs when model updates
- Manages reconciliation tasks

**demo_psp_ring1_init_job.yaml** - Kubernetes Job that:

- Initializes database schemas
- Creates required tables in merge database
- Sets up Ring 1 infrastructure

**demo_psp_model_merge_job.yaml** - Kubernetes Job that:

- Processes model changes
- Updates pipeline configurations
- Synchronizes state with merge database

## Deployment Order

After generating Ring 0 artifacts:

```bash
# 1. Apply Kubernetes bootstrap (creates PVC, MCP server, network policies)
kubectl apply -f generated_output/Demo_PSP/kubernetes-bootstrap.yaml

# 2. Copy DAG to GitSync repository
cp generated_output/Demo_PSP/demo_psp_infrastructure_dag.py /path/to/demo1_airflow/dags/
cd /path/to/demo1_airflow
git add dags/
git commit -m "Add infrastructure DAG"
git push

# 3. Run Ring 1 initialization (creates database schemas)
kubectl apply -f generated_output/Demo_PSP/demo_psp_ring1_init_job.yaml

# 4. Wait for Ring 1 to complete
kubectl wait --for=condition=complete job/demo-psp-ring1-init -n $NAMESPACE --timeout=120s

# 5. Run model merge job
kubectl apply -f generated_output/Demo_PSP/demo_psp_model_merge_job.yaml
```

## Regenerating Artifacts

If you update your model (eco.py, rte_demo.py), regenerate artifacts:

```bash
# Remove old artifacts
rm -rf generated_output/

# Regenerate
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Demo_PSP \
  --rte-name demo

# Reapply to Kubernetes
kubectl apply -f generated_output/Demo_PSP/kubernetes-bootstrap.yaml

# Delete and recreate jobs (jobs are immutable)
kubectl delete job demo-psp-ring1-init -n $NAMESPACE --ignore-not-found
kubectl delete job demo-psp-model-merge-job -n $NAMESPACE --ignore-not-found
kubectl apply -f generated_output/Demo_PSP/demo_psp_ring1_init_job.yaml
kubectl apply -f generated_output/Demo_PSP/demo_psp_model_merge_job.yaml
```

## Using a Newer Image Version

To use a different DataSurface version:

```bash
# Set version
export DATASURFACE_VERSION="1.2.0"

# Pull new image
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}

# Update rte_demo.py to use new image
# datasurfaceDockerImage="registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.2.0"

# Regenerate artifacts
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Demo_PSP \
  --rte-name demo
```

## Troubleshooting

### Model Import Errors

**Error:**

```text
ModuleNotFoundError: No module named 'eco'
ImportError: cannot import name 'Ecosystem' from 'eco'
```

**Solution:** Ensure you're running from the model directory and the volume mount is correct:

```bash
cd /path/to/demo1_actual
docker run --rm -v "$(pwd)":/workspace/model ...
```

### PSP Not Found

**Error:**

```text
ValueError: PSP 'Demo_PSP' not found in ecosystem
```

**Solution:** Check the PSP name matches exactly what's defined in your model. Look in `rte_demo.py` for:

```python
YellowPlatformServiceProvider(
    "Demo_PSP",
    ...
)
```

### RTE Not Found

**Error:**

```text
ValueError: RTE 'demo' not found
```

**Solution:** Check the RTE name matches the runtime environment in your model:

```python
YellowRunTimeEnvironment(
    "demo",
    ...
)
```

### Permission Denied on Output

**Error:**

```text
PermissionError: [Errno 13] Permission denied: '/workspace/model/generated_output'
```

**Solution:** The Docker container runs as a specific user. Either:

1. Pre-create the output directory: `mkdir -p generated_output`
2. Or run with user mapping: `docker run --rm -u $(id -u):$(id -g) -v ...`

### NoneType Errors

**Error:**

```text
AttributeError: 'NoneType' object has no attribute 'name'
```

**Solution:** Usually indicates a missing or misconfigured credential in the model. Verify all credentials are defined:

```python
mergeRW_Credential=Credential("postgres-demo-merge", CredentialType.USER_PASSWORD)
gitPlatformRepoCredential=Credential("git", CredentialType.API_TOKEN)
```

## Comparing Generated Artifacts

To compare artifacts after regeneration (useful when debugging):

```bash
# Save old artifacts
cp -r generated_output/Demo_PSP generated_output/Demo_PSP_old

# Regenerate
docker run --rm ...

# Compare
diff generated_output/Demo_PSP_old/demo_psp_model_merge_job.yaml \
     generated_output/Demo_PSP/demo_psp_model_merge_job.yaml
```

## Validating Generated YAML

Before applying to Kubernetes:

```bash
# Dry-run to check for errors
kubectl apply -f generated_output/Demo_PSP/kubernetes-bootstrap.yaml --dry-run=client

# Validate YAML syntax
kubectl apply -f generated_output/Demo_PSP/demo_psp_ring1_init_job.yaml --dry-run=server
```
