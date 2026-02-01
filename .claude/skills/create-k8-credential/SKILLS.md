---
name: Create a DataSurface Yellow Kubernetes Credential
description: Creates a Kubernetes credential for DataSurface Yellow environment.
---
# K8 secret naming rules

Yellow encodes the model credential name to be compatible with Kubernetes secret naming rules.

## Overview

Yellow uses a `KubernetesEnvVarsCredentialStore` to manage credentials. This store retrieves credentials from Kubernetes secrets that are injected into pods as environment variables. Understanding how to properly create and name these secrets is essential for Yellow to function correctly.

## How Yellow Retrieves Credentials

When a Yellow job runs in Kubernetes, it needs credentials for:

- **Merge database** access (read/write)
- **Git repository** access (for model retrieval)
- **Source databases** (for ingestion)
- **CQRS target databases** (for data replication)
- **DataTransformer execution** credentials

Yellow converts credential names to Kubernetes-compatible names using a specific convention, then looks for environment variables with standard suffixes based on the credential type.

## Naming Convention

Yellow uses `K8sUtils.to_k8s_name()` to convert credential names to Kubernetes-compatible names:

1. Convert to **lowercase**
2. Replace **underscores** (`_`) with **hyphens** (`-`)
3. Replace **spaces** with **hyphens** (`-`)
4. Remove any non-alphanumeric characters (except hyphens)
5. Collapse multiple consecutive hyphens into one
6. Strip leading/trailing hyphens

### Examples

| Model Credential Name | K8s Secret Name |
| ----------------------- | ----------------- |
| `postgres` | `postgres` |
| `git` | `git` |
| `My_Database_Cred` | `my-database-cred` |
| `SQL Server` | `sql-server` |
| `AWS_S3_Access` | `aws-s3-access` |

## Supported Credential Types

Yellow supports four credential types, each with specific environment variable suffixes:

| Credential Type | Description | Environment Variables |
| ----------------- | ------------- | ---------------------- |
| `USER_PASSWORD` | Username and password | `{name}_USER`, `{name}_PASSWORD` |
| `API_TOKEN` | Single token/key | `{name}_TOKEN` |
| `CLIENT_CERT_WITH_KEY` | Certificate with private key | `{name}_USER`, `{name}_PRIVATE_KEY`, `{name}_PASSPHRASE` |
| `API_KEY_PAIR` | Public/private key pair | `{name}_PUB`, `{name}_PRV`, `{name}_PWD` |

**Important:** The environment variable names use the K8s-converted secret name as a prefix (e.g., `postgres-demo_USER` for a credential named `postgres-demo`).

## Creating Kubernetes Secrets

### USER_PASSWORD Credentials

Used for database connections (PostgreSQL, SQL Server, MySQL, etc.).

**Model Definition:**

```python
from datasurface.security import Credential, CredentialType

db_cred = Credential("postgres-demo", CredentialType.USER_PASSWORD)
```

**Create the Secret:**

```bash
# Model name: "postgres-demo" → K8s secret: "postgres-demo" (no conversion needed)
kubectl create secret generic postgres-demo \
  --from-literal=USER=myuser \
  --from-literal=PASSWORD=mypassword \
  -n <namespace>
```

**Environment Variables Injected:**

- `postgres-demo_USER=myuser`
- `postgres-demo_PASSWORD=mypassword`

**More Examples:**

```python
# Model: Credential("sqlserver", CredentialType.USER_PASSWORD)
# Model: Credential("Mask_DT_Cred", CredentialType.USER_PASSWORD)
```

```bash
# Model name: "sqlserver" → K8s secret: "sqlserver"
kubectl create secret generic sqlserver \
  --from-literal=USER=sa \
  --from-literal=PASSWORD='YourStr0ngP@ssword' \
  -n <namespace>

# Model name: "Mask_DT_Cred" → K8s secret: "mask-dt-cred"
kubectl create secret generic mask-dt-cred \
  --from-literal=USER=dt_user \
  --from-literal=PASSWORD=dt_password \
  -n <namespace>
```

### API_TOKEN Credentials

Used for Git repository access, API services, and Kafka authentication.

**Model Definition:**

```python
git_cred = Credential("git", CredentialType.API_TOKEN)
```

**Create the Secret:**

```bash
# Model name: "git" → K8s secret: "git" (no conversion needed)
kubectl create secret generic git \
  --from-literal=TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx \
  -n <namespace>
```

**Environment Variables Injected:**

- `git_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx`

**More Examples:**

```python
# Model: Credential("slack", CredentialType.API_TOKEN)
# Model: Credential("connect", CredentialType.API_TOKEN)
```

```bash
# Model name: "slack" → K8s secret: "slack"
kubectl create secret generic slack \
  --from-literal=TOKEN=xoxb-your-slack-token \
  -n <namespace>

# Model name: "connect" → K8s secret: "connect"
kubectl create secret generic connect \
  --from-literal=TOKEN=your-kafka-token \
  -n <namespace>
```

### CLIENT_CERT_WITH_KEY Credentials

Used for key-pair authentication (e.g., Snowflake with RSA keys).

**Model Definition:**

```python
snowflake_cred = Credential("Snowflake_Prod", CredentialType.CLIENT_CERT_WITH_KEY)
```

**Create the Secret:**

```bash
# Model name: "Snowflake_Prod" → K8s secret: "snowflake-prod"
kubectl create secret generic snowflake-prod \
  --from-literal=USER=snowflake_user \
  --from-literal=PRIVATE_KEY="$(cat /path/to/private_key.pem)" \
  --from-literal=PASSPHRASE=optional_key_passphrase \
  -n <namespace>
```

**Environment Variables Injected:**

- `snowflake-prod_USER=snowflake_user`
- `snowflake-prod_PRIVATE_KEY=<contents of private key>`
- `snowflake-prod_PASSPHRASE=optional_key_passphrase`

**Note:** The passphrase can be empty for unencrypted private keys.

### API_KEY_PAIR Credentials

Used for services requiring both public and private keys (e.g., AWS-style credentials).

**Model Definition:**

```python
aws_cred = Credential("AWS_S3_Access", CredentialType.API_KEY_PAIR)
```

**Create the Secret:**

```bash
# Model name: "AWS_S3_Access" → K8s secret: "aws-s3-access"
kubectl create secret generic aws-s3-access \
  --from-literal=PUB=AKIAIOSFODNN7EXAMPLE \
  --from-literal=PRV=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --from-literal=PWD= \
  -n <namespace>
```

**Environment Variables Injected:**

- `aws-s3-access_PUB=AKIAIOSFODNN7EXAMPLE`
- `aws-s3-access_PRV=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
- `aws-s3-access_PWD=`

## Complete Example

Here's a complete example setting up credentials for a typical Yellow deployment.

**Model Definitions:**

```python
from datasurface.security import Credential, CredentialType

# These credentials would be defined in your ecosystem model
postgres_cred = Credential("postgres", CredentialType.USER_PASSWORD)
git_cred = Credential("git", CredentialType.API_TOKEN)
source_db_cred = Credential("Source_DB", CredentialType.USER_PASSWORD)
cqrs_cred = Credential("CQRS_Postgres", CredentialType.USER_PASSWORD)
dt_cred = Credential("My_DataTransformer", CredentialType.USER_PASSWORD)
snowflake_cred = Credential("Snowflake_Target", CredentialType.CLIENT_CERT_WITH_KEY)
```

**Create Kubernetes Secrets:**

```bash
NAMESPACE=yp-airflow3

# Model: "postgres" → K8s: "postgres"
kubectl create secret generic postgres \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=your_merge_db_password \
  -n $NAMESPACE

# Model: "git" → K8s: "git"
kubectl create secret generic git \
  --from-literal=TOKEN=ghp_your_github_personal_access_token \
  -n $NAMESPACE

# Model: "Source_DB" → K8s: "source-db"
kubectl create secret generic source-db \
  --from-literal=USER=reader \
  --from-literal=PASSWORD=reader_password \
  -n $NAMESPACE

# Model: "CQRS_Postgres" → K8s: "cqrs-postgres"
kubectl create secret generic cqrs-postgres \
  --from-literal=USER=cqrs_writer \
  --from-literal=PASSWORD=cqrs_password \
  -n $NAMESPACE

# Model: "My_DataTransformer" → K8s: "my-datatransformer"
kubectl create secret generic my-datatransformer \
  --from-literal=USER=dt_user \
  --from-literal=PASSWORD=dt_password \
  -n $NAMESPACE

# Model: "Snowflake_Target" → K8s: "snowflake-target"
kubectl create secret generic snowflake-target \
  --from-literal=USER=SNOWFLAKE_USER \
  --from-literal=PRIVATE_KEY="$(cat ~/.ssh/snowflake_key.pem)" \
  --from-literal=PASSPHRASE=my_key_passphrase \
  -n $NAMESPACE
```

## Updating Secrets

To update an existing secret without deleting it:

```bash
kubectl create secret generic postgres \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=new_password \
  -n $NAMESPACE \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Verifying Secrets

Check that secrets exist and have the expected keys:

```bash
# List secrets
kubectl get secrets -n $NAMESPACE

# View secret keys (not values)
kubectl describe secret postgres -n $NAMESPACE

# Decode a secret value (base64)
kubectl get secret postgres -n $NAMESPACE -o jsonpath='{.data.USER}' | base64 -d
```

## Troubleshooting

### Common Issues

1. **Credential not found**: Check that the secret name matches the K8s-converted credential name (lowercase, underscores to hyphens).

2. **Wrong key names**: Ensure you use the correct suffixes (`USER`, `PASSWORD`, `TOKEN`, etc.) - they are case-sensitive.

3. **Namespace mismatch**: Secrets must be in the same namespace as the Yellow pods.

4. **Special characters in passwords**: Use single quotes around values with special characters:

   ```bash
   kubectl create secret generic db \
     --from-literal=USER=user \
     --from-literal=PASSWORD='P@ss!word#123' \
     -n $NAMESPACE
   ```

### Debugging Credential Issues

Check pod logs for credential errors:

```bash
kubectl logs <pod-name> -n $NAMESPACE | grep -i credential
```

Verify environment variables in a running pod:

```bash
kubectl exec -it <pod-name> -n $NAMESPACE -- env | grep -E '_(USER|PASSWORD|TOKEN)$'
```

## Security Best Practices

1. **Use Kubernetes RBAC** to restrict who can read secrets.

2. **Enable encryption at rest** for etcd to protect secrets.

3. **Rotate credentials regularly** using the update pattern above.

4. **Use separate credentials** for different purposes (don't reuse the merge DB credential for sources).

5. **Consider external secret managers** (AWS Secrets Manager, HashiCorp Vault) for production environments - Yellow's AWS assembly supports AWS Secrets Manager natively.
