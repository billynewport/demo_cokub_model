# Datasurface artifacts

## Customer Artifact Access

This guide explains how to configure access to DataSurface Docker images and Python modules.

## Prerequisites

You will receive the following credentials from DataSurface:

- **Username**: Your deploy token username (e.g., `customer-acme`)
- **Token**: Your deploy token value
- **Project ID**: `77796931`

## Docker Images

### Available Images

- `registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}` — Core DataSurface image
- `registry.gitlab.com/datasurface-inc/datasurface/datasurface-dbt:v${DATASURFACE_VERSION}` — DataSurface with dbt support

### Local Docker Pull

```bash
# Login to GitLab registry (one-time)
docker login registry.gitlab.com -u <username> -p <token>

# Pull images
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface-dbt:v${DATASURFACE_VERSION}
```

### Kubernetes Configuration

Create an image pull secret:

```bash
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username=<username> \
  --docker-password=<token> \
  -n <namespace>
```

Reference the secret in your pods or deployments:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: datasurface-pod
spec:
  imagePullSecrets:
    - name: datasurface-registry
  containers:
    - name: datasurface
      image: registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
```

For Helm charts, add to your values:

```yaml
imagePullSecrets:
  - name: datasurface-registry

image:
  repository: registry.gitlab.com/datasurface-inc/datasurface/datasurface
  tag: v${DATASURFACE_VERSION}
```

## Python Module

### Install with pip

```bash
pip install datasurface \
  --index-url https://<username>:<token>@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
```

### Install a Specific Version

```bash
pip install datasurface==${DATASURFACE_VERSION} \
  --index-url https://<username>:<token>@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
```

### Configure pip.conf

For persistent configuration, add to `~/.pip/pip.conf` (Linux/macOS) or `%APPDATA%\pip\pip.ini` (Windows):

```ini
[global]
extra-index-url = https://<username>:<token>@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
```

### Requirements File

Add the index URL to your `requirements.txt`:

```text
--extra-index-url https://<username>:<token>@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
datasurface==${DATASURFACE_VERSION}
```

### Environment Variable (CI/CD)

For CI/CD pipelines, set credentials as environment variables:

```bash
export PIP_INDEX_URL="https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
pip install datasurface
```

## Optional Dependencies

The base `datasurface` package works on both AMD64 and ARM64. For DB2 support (AMD64 only):

```bash
pip install datasurface[db2] \
  --index-url https://<username>:<token>@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
```

## Version Requirements

- Python >= 3.12, this is due to sqlserver and snowflake dependencies.

## Troubleshooting

### Authentication Failed

Verify your credentials:

```bash
# Test Docker login
docker login registry.gitlab.com -u <username> -p <token>

# Test PyPI access (should list available versions)
pip index versions datasurface \
  --index-url https://<username>:<token>@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
```

### Token Expired

Contact DataSurface support to renew your access token.

### Wrong Python Version

DataSurface requires Python 3.12 or higher:

```bash
python --version  # Should show 3.12.x or higher
```

## Security Notes

- Do not commit tokens to version control
- Use environment variables or secrets management in CI/CD
- Rotate tokens periodically
- Report any suspected token compromise immediately

---
