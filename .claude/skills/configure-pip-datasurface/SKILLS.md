---
name: Configure pip for DataSurface
description: Set up persistent pip configuration for DataSurface package access.
---
# Configure pip for DataSurface

Set up persistent pip configuration so you don't need to specify the index URL every time.

## Prerequisites

- GitLab PyPI credentials:
  - `DATASURFACE_USER` - Your deploy token username
  - `DATASURFACE_TOKEN` - Your deploy token value
  - Project ID: `77796931`

## Option 1: pip.conf (Recommended for Development)

### Linux/macOS

Create or edit `~/.pip/pip.conf`:

```bash
mkdir -p ~/.pip
cat > ~/.pip/pip.conf << EOF
[global]
extra-index-url = https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
EOF
```

### Windows

Create or edit `%APPDATA%\pip\pip.ini`:

```ini
[global]
extra-index-url = https://USERNAME:TOKEN@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
```

### Verify Configuration

```bash
pip config list
pip install datasurface  # Should work without --index-url
```

## Option 2: requirements.txt

Add the index URL to your `requirements.txt`:

```text
--extra-index-url https://USERNAME:TOKEN@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
datasurface==1.1.0
```

Then install:

```bash
pip install -r requirements.txt
```

**Security note:** Don't commit requirements.txt with credentials. Use environment variable substitution in CI/CD.

## Option 3: Environment Variable (CI/CD)

Set the index URL as an environment variable:

```bash
export PIP_EXTRA_INDEX_URL="https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

Then pip will use it automatically:

```bash
pip install datasurface
```

### GitHub Actions Example

```yaml
- name: Install DataSurface
  env:
    PIP_EXTRA_INDEX_URL: https://${{ secrets.DATASURFACE_USER }}:${{ secrets.DATASURFACE_TOKEN }}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple
  run: pip install datasurface
```

### GitLab CI Example

```yaml
install:
  script:
    - pip install datasurface
  variables:
    PIP_EXTRA_INDEX_URL: "https://${DATASURFACE_USER}:${DATASURFACE_TOKEN}@gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
```

## Option 4: pyproject.toml (Poetry)

For Poetry projects, add to `pyproject.toml`:

```toml
[[tool.poetry.source]]
name = "datasurface"
url = "https://gitlab.com/api/v4/projects/77796931/packages/pypi/simple"
priority = "supplemental"
```

Configure credentials:

```bash
poetry config http-basic.datasurface $DATASURFACE_USER $DATASURFACE_TOKEN
```

## Removing Configuration

### pip.conf

```bash
rm ~/.pip/pip.conf
# or edit and remove the extra-index-url line
```

### Environment Variable

```bash
unset PIP_EXTRA_INDEX_URL
```

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use environment variables** in CI/CD pipelines
3. **Use secrets managers** (AWS Secrets Manager, HashiCorp Vault) for production
4. **Rotate tokens** periodically
5. **Restrict pip.conf permissions**:

   ```bash
   chmod 600 ~/.pip/pip.conf
   ```

## Troubleshooting

### Configuration Not Being Used

Check pip configuration:

```bash
pip config list
pip config debug
```

### Multiple Index URLs Conflicting

Use `extra-index-url` (not `index-url`) to add DataSurface registry alongside PyPI:

```ini
[global]
extra-index-url = https://...
```

Using `index-url` would replace PyPI entirely.

### Token Contains Special Characters

URL-encode special characters in the token:

- `@` becomes `%40`
- `:` becomes `%3A`
- `/` becomes `%2F`

Or use the environment variable approach which handles encoding automatically.
