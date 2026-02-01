---
name: Install DataSurface Python Module
description: Install the DataSurface Python package from GitLab PyPI registry.
---
# Install DataSurface Python Module

Install the DataSurface Python package from the GitLab package registry.

## Prerequisites

- Python >= 3.12 (required for SQL Server and Snowflake dependencies)
- GitLab PyPI credentials:
  - `DATASURFACE_USER` - Your deploy token username
  - `DATASURFACE_TOKEN` - Your deploy token value
  - Project ID: `77796931`

## Check Python Version

```bash
python --version
# Must be 3.12.x or higher
```

## Steps

### 1. Set Environment Variables

```bash
export DATASURFACE_USER="your-deploy-token-username"
export DATASURFACE_TOKEN="your-deploy-token"
export DATASURFACE_VERSION="1.1.0"
```

### 2. Install with pip

**Latest version:**

```bash
pip install datasurface \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

**Specific version:**

```bash
pip install datasurface==${DATASURFACE_VERSION} \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

### 3. Verify Installation

```bash
pip show datasurface
python -c "import datasurface; print(datasurface.__version__)"
```

## Optional Dependencies

**With DB2 support (AMD64 only):**

```bash
pip install "datasurface[db2]" \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

## Quick One-Liner

```bash
pip install datasurface --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

## Using in Virtual Environment

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or: .venv\Scripts\activate  # Windows

# Install
pip install datasurface \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

## Upgrading

```bash
pip install --upgrade datasurface \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

## Troubleshooting

### Authentication Failed

```text
ERROR: 401 Client Error: Unauthorized
```

**Solution:** Verify credentials:

```bash
# Test access (should list versions)
pip index versions datasurface \
  --index-url "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

### Wrong Python Version

```text
ERROR: Package requires Python >=3.12
```

**Solution:** Use Python 3.12 or higher:

```bash
python3.12 -m pip install datasurface ...
# or use pyenv/conda to switch Python versions
```

### Package Not Found

```text
ERROR: No matching distribution found for datasurface
```

**Solution:** Check the index URL is correct and credentials are valid:

```bash
# Verify URL format
echo "https://${DATASURFACE_USER}:****@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

### SSL Certificate Error

```text
SSLError: certificate verify failed
```

**Solution:** Update certificates:

```bash
pip install --upgrade certifi
```

## Security Note

Never commit credentials to version control. Use environment variables or a secrets manager.
