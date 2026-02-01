---
name: Pull DataSurface Docker Image
description: Pull DataSurface Docker images from GitLab registry to local machine.
---
# Pull DataSurface Docker Image

Pull DataSurface Docker images from the GitLab container registry to your local machine.

## Prerequisites

You need GitLab registry credentials:

- `GITLAB_CUSTOMER_USER` - Your deploy token username
- `GITLAB_CUSTOMER_TOKEN` - Your deploy token value

## Available Images

| Image | Description |
|-------|-------------|
| `datasurface` | Core DataSurface image |
| `datasurface-dbt` | DataSurface with dbt support |

## Steps

### 1. Set Environment Variables

```bash
export GITLAB_CUSTOMER_USER="your-deploy-token-username"
export GITLAB_CUSTOMER_TOKEN="your-deploy-token"
export DATASURFACE_VERSION="1.1.0"
```

### 2. Login to GitLab Registry

```bash
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"
```

Expected output:

```text
Login Succeeded
```

### 3. Pull the Image

**Core image:**

```bash
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
```

**With dbt support:**

```bash
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface-dbt:v${DATASURFACE_VERSION}
```

### 4. Verify the Image

```bash
docker images | grep datasurface
```

Expected output:

```text
registry.gitlab.com/datasurface-inc/datasurface/datasurface   v1.1.0   abc123def456   2 days ago   1.2GB
```

## Quick One-Liner

```bash
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN" && \
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
```

## Troubleshooting

### Authentication Failed

```text
Error response from daemon: Get "https://registry.gitlab.com/v2/": denied: access forbidden
```

**Solution:** Verify credentials are correct:

```bash
echo "User: $GITLAB_CUSTOMER_USER"
echo "Token length: ${#GITLAB_CUSTOMER_TOKEN}"
```

### Image Not Found

```text
Error response from daemon: manifest for ... not found
```

**Solution:** Check available versions or verify the version number:

```bash
# Try latest stable version
export DATASURFACE_VERSION="1.1.0"
```

### Pulling Latest Version

To get the most recent image after a fix:

```bash
# Force re-pull even if tag exists locally
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}

# Or remove and re-pull
docker rmi registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
```
