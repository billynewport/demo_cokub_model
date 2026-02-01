---
name: Troubleshoot DataSurface Authentication
description: Debug authentication failures for DataSurface Docker registry and PyPI access.
---
# Troubleshoot DataSurface Authentication

Diagnose and fix authentication issues when accessing DataSurface Docker images and Python packages.

## Quick Diagnosis

### Test Docker Registry Access

```bash
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"
```

**Success:**

```text
Login Succeeded
```

**Failure:**

```text
Error response from daemon: Get "https://registry.gitlab.com/v2/": denied: access forbidden
```

### Test PyPI Access

```bash
pip index versions datasurface \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

**Success:** Lists available versions
**Failure:** 401 Unauthorized or connection error

## Common Issues

### 1. Invalid Credentials

**Symptoms:**

- `401 Unauthorized`
- `access forbidden`
- `denied: access forbidden`

**Diagnosis:**

```bash
# Check credentials are set
echo "Docker User: $GITLAB_CUSTOMER_USER"
echo "Docker Token length: ${#GITLAB_CUSTOMER_TOKEN}"
echo "PyPI User: $DATASURFACE_USER"
echo "PyPI Token length: ${#DATASURFACE_TOKEN}"
```

**Solutions:**

1. Verify credentials are exported:

```bash
export GITLAB_CUSTOMER_USER="your-username"
export GITLAB_CUSTOMER_TOKEN="your-token"
```

2. Check for trailing whitespace or newlines:

```bash
# Remove any trailing whitespace
export GITLAB_CUSTOMER_TOKEN=$(echo "$GITLAB_CUSTOMER_TOKEN" | tr -d '[:space:]')
```

3. Verify token hasn't expired - contact DataSurface support for new credentials

### 2. Token Expired

**Symptoms:**

- Previously working credentials now fail
- `401 Unauthorized`

**Solution:**
Contact DataSurface support to renew your deploy token.

### 3. Wrong Project ID

**Symptoms:**

- `404 Not Found` for PyPI
- Package not found

**Diagnosis:**

```bash
# Verify project ID in URL
echo "https://.../projects/77796931/packages/pypi/simple"
#                         ^^^^^^^^ should be 77796931
```

**Solution:**
Use the correct project ID: `77796931`

### 4. Special Characters in Token

**Symptoms:**

- Auth fails even with correct credentials
- URL parsing errors

**Diagnosis:**

```bash
# Check for special characters
echo "$GITLAB_CUSTOMER_TOKEN" | grep -E '[@:/%]'
```

**Solutions:**

For Docker:

```bash
# Use stdin to avoid shell interpretation
echo "$GITLAB_CUSTOMER_TOKEN" | docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" --password-stdin
```

For pip, URL-encode special characters:

- `@` → `%40`
- `:` → `%3A`
- `/` → `%2F`
- `%` → `%25`

Or use environment variable:

```bash
export PIP_EXTRA_INDEX_URL="https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
pip install datasurface
```

### 5. Network/Firewall Issues

**Symptoms:**

- Connection timeout
- `Could not connect to registry.gitlab.com`

**Diagnosis:**

```bash
# Test connectivity
curl -I https://registry.gitlab.com
curl -I https://gitlab.com

# Check DNS resolution
nslookup registry.gitlab.com
nslookup gitlab.com
```

**Solutions:**

1. Check firewall allows HTTPS (port 443) to:
   - `registry.gitlab.com`
   - `gitlab.com`

2. If behind corporate proxy:

```bash
export HTTPS_PROXY=http://proxy.company.com:8080
export HTTP_PROXY=http://proxy.company.com:8080
```

3. For Docker behind proxy, configure Docker daemon:

```json
{
  "proxies": {
    "default": {
      "httpProxy": "http://proxy.company.com:8080",
      "httpsProxy": "http://proxy.company.com:8080"
    }
  }
}
```

### 6. SSL Certificate Errors

**Symptoms:**

- `SSL: CERTIFICATE_VERIFY_FAILED`
- `certificate verify failed`

**Solutions:**

For pip:

```bash
pip install --upgrade certifi
```

For corporate environments with custom CA:

```bash
export REQUESTS_CA_BUNDLE=/path/to/corporate-ca-bundle.crt
pip install datasurface ...
```

For Docker:

```bash
# Add corporate CA to Docker's trust store
# Location varies by OS - consult Docker documentation
```

### 7. Kubernetes ImagePullBackOff

**Symptoms:**

- Pods stuck in `ImagePullBackOff`
- Events show authentication errors

**Diagnosis:**

```bash
kubectl describe pod <pod-name> -n $NAMESPACE | grep -A10 Events
kubectl get secret datasurface-registry -n $NAMESPACE
kubectl get sa default -n $NAMESPACE -o yaml | grep -A2 imagePullSecrets
```

**Solutions:**

1. Secret doesn't exist - create it:

```bash
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE
```

2. Secret not attached to service account:

```bash
kubectl patch serviceaccount default -n $NAMESPACE \
  -p '{"imagePullSecrets": [{"name": "datasurface-registry"}]}'
```

3. Secret has wrong credentials - recreate:

```bash
kubectl delete secret datasurface-registry -n $NAMESPACE
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE
```

## Verification Checklist

Run through this checklist to verify everything works:

```bash
# 1. Environment variables set
echo "GITLAB_CUSTOMER_USER: ${GITLAB_CUSTOMER_USER:-(not set)}"
echo "GITLAB_CUSTOMER_TOKEN: ${GITLAB_CUSTOMER_TOKEN:+(set)}"
echo "DATASURFACE_USER: ${DATASURFACE_USER:-(not set)}"
echo "DATASURFACE_TOKEN: ${DATASURFACE_TOKEN:+(set)}"

# 2. Docker registry login
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"

# 3. Docker pull
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0

# 4. PyPI access
pip index versions datasurface \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"

# 5. Kubernetes secret (if applicable)
kubectl get secret datasurface-registry -n $NAMESPACE
kubectl get sa default -n $NAMESPACE -o jsonpath='{.imagePullSecrets[*].name}'
```

## Getting Help

If issues persist after trying these solutions:

1. Collect diagnostic info:

```bash
docker version
pip --version
python --version
kubectl version --client
```

2. Note the exact error message

3. Contact DataSurface support with:

   - Error message
   - Which access method fails (Docker, pip, Kubernetes)
   - Environment (local, CI/CD, Kubernetes)
