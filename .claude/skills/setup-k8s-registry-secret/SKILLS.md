---
name: Setup Kubernetes Registry Secret
description: Configure Kubernetes to pull DataSurface images from GitLab registry.
---
# Setup Kubernetes Registry Secret

Configure your Kubernetes cluster to pull DataSurface Docker images from the GitLab container registry.

## Prerequisites

- `kubectl` configured and connected to your cluster
- Kubernetes namespace created
- GitLab registry credentials:
  - `GITLAB_CUSTOMER_USER` - Your deploy token username
  - `GITLAB_CUSTOMER_TOKEN` - Your deploy token value

## Steps

### 1. Set Environment Variables

```bash
export NAMESPACE="demo1"
export GITLAB_CUSTOMER_USER="your-deploy-token-username"
export GITLAB_CUSTOMER_TOKEN="your-deploy-token"
```

### 2. Create the Docker Registry Secret

```bash
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE
```

Expected output:

```text
secret/datasurface-registry created
```

### 3. Attach Secret to Default Service Account

This allows all pods in the namespace to use the secret automatically:

```bash
kubectl patch serviceaccount default -n $NAMESPACE \
  -p '{"imagePullSecrets": [{"name": "datasurface-registry"}]}'
```

Expected output:

```text
serviceaccount/default patched
```

### 4. Verify Configuration

```bash
# Check secret exists
kubectl get secret datasurface-registry -n $NAMESPACE

# Verify service account has imagePullSecrets
kubectl get sa default -n $NAMESPACE -o yaml | grep -A2 imagePullSecrets
```

Expected output:

```text
imagePullSecrets:
- name: datasurface-registry
```

## Using in Pod Specs

If not using the default service account, reference the secret directly:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: datasurface-pod
  namespace: demo1
spec:
  imagePullSecrets:
    - name: datasurface-registry
  containers:
    - name: datasurface
      image: registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0
```

## Using in Helm Charts

Add to your Helm values:

```yaml
imagePullSecrets:
  - name: datasurface-registry

image:
  repository: registry.gitlab.com/datasurface-inc/datasurface/datasurface
  tag: v1.1.0
```

## Updating the Secret

If credentials change, delete and recreate:

```bash
kubectl delete secret datasurface-registry -n $NAMESPACE

kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE
```

Or use dry-run with apply:

```bash
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Troubleshooting

### ImagePullBackOff

If pods show `ImagePullBackOff`:

```bash
# Check pod events
kubectl describe pod <pod-name> -n $NAMESPACE | grep -A10 Events

# Verify secret exists
kubectl get secret datasurface-registry -n $NAMESPACE

# Verify service account configuration
kubectl get sa default -n $NAMESPACE -o yaml
```

### Secret Not Found in Pod

Ensure the pod is using the default service account, or explicitly reference the secret:

```yaml
spec:
  imagePullSecrets:
    - name: datasurface-registry
```

### Invalid Credentials

Test credentials locally first:

```bash
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"
```

If local login fails, the credentials are invalid.
