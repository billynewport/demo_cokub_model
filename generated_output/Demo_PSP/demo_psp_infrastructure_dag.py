"""
Infrastructure DAG for Demo_PSP Data Platform
Generated automatically by DataSurface Yellow Airflow 3.x Platform

This DAG contains the core infrastructure tasks:
- MERGE task: Generates infrastructure terraform files
- Reconcile views task: Reconciles workspace views
- Metrics collector task: Collects and processes metrics
- Apply security task: Applies security configurations
- Table removal task: Cleans up unused tables

Airflow 3.x Changes:
- Operators moved to airflow.providers.standard package
- provide_context parameter removed (always provided)
- Variable access replaced with XCom for task communication
- State import updated to TaskInstanceState
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# Airflow 3.x: Standard operators moved to airflow.providers.standard package
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import json
import os
import logging
import base64
import re
import urllib.request
import urllib.error
from kubernetes import client, config
from kubernetes.client import models as k8s
from typing import Optional, Union, TypedDict, NotRequired, List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from abc import ABC, abstractmethod
from typing import Tuple
from airflow.utils.dag_parsing_context import get_parsing_context

# Airflow 3.x: Use standard Python logging instead of task_instance.log
_logger = logging.getLogger(__name__)

# Module-level flag to suppress verbose logging during task execution
# Set based on parsing context at module load time
_silent_mode = False

# Sharding configuration for parallel DAG file processing
# When num_shards > 1, each DAG file only loads DAGs where dag_hash % num_shards == shard_number
_num_shards = 1
_shard_number = 0


def _get_shard_filter() -> str:
    """Return SQL WHERE clause fragment for shard filtering, or empty string if not sharded."""
    if _num_shards > 1:
        return f" AND dag_hash % {_num_shards} = {_shard_number}"
    return ""


class SecretManager(ABC):
    @abstractmethod
    def getUserPassword(self, credential_name: str) -> Tuple[Union[k8s.V1EnvVarSource, str], Union[k8s.V1EnvVarSource, str]]:
        """Return Kubernetes EnvVarSource objects for USER and PASSWORD for a credential"""
        pass

    @abstractmethod
    def getApiKey(self, credential_name: str) -> Tuple[Union[k8s.V1EnvVarSource, str], Union[k8s.V1EnvVarSource, str]]:
        """Return Kubernetes EnvVarSource objects for API key and secret"""
        pass

    @abstractmethod
    def getPATSecret(self, credential_name: str) -> Union[k8s.V1EnvVarSource, str]:
        """Return a Kubernetes EnvVarSource for a personal access token"""
        pass

    @staticmethod
    def to_envvar(name: str, val: Union[str, k8s.V1EnvVarSource]) -> k8s.V1EnvVar:
        return k8s.V1EnvVar(name=name, value=val) if isinstance(val, str) else k8s.V1EnvVar(name=name, value_from=val)

# Airflow 3.x Kubernetes Secret Manager
# This version reads secrets directly from the Kubernetes API,
# which is necessary because Airflow 3.x's SDK task runner runs in a subprocess
# that doesn't inherit environment variables from the pod.
#
# RBAC permissions required:
# - The Airflow service account must have 'get' permission on secrets in the namespace

import base64
from typing import Dict, Optional, Tuple
from kubernetes import client, config
from kubernetes.client import models as k8s


class K8sSecretManager(SecretManager):
    """
    Kubernetes Secret Manager that reads secrets directly from the Kubernetes API.
    
    This allows the DAG code running inside Airflow (scheduler, workers) to access
    secrets dynamically, similar to how AWS/Azure secret managers work.
    
    The Airflow service account must have RBAC permissions to read secrets in the namespace.
    """
    
    def __init__(self, namespace: str):
        super().__init__()
        self.namespace = namespace
        self._api: Optional[client.CoreV1Api] = None
    
    @property
    def api(self) -> client.CoreV1Api:
        """Lazily initialize the Kubernetes API client."""
        if self._api is None:
            try:
                # Try in-cluster config first (when running inside Kubernetes)
                config.load_incluster_config()
            except config.ConfigException:
                # Fall back to kubeconfig for local development
                config.load_kube_config()
            self._api = client.CoreV1Api()
        return self._api
    
    def _normalize_secret_name(self, name: str) -> str:
        """Normalize credential name to valid K8s secret name (RFC 1123).
        
        Must match K8sUtils.to_k8s_name() logic exactly:
        1. Convert to lowercase
        2. Replace underscores with hyphens
        3. Replace spaces with hyphens  
        4. Remove non-alphanumeric characters (except hyphens)
        5. Collapse multiple consecutive hyphens
        6. Strip leading/trailing hyphens
        """
        import re
        name = name.lower().replace('_', '-').replace(' ', '-')
        name = re.sub(r'[^a-z0-9-]', '', name)
        name = re.sub(r'-+', '-', name)
        name = name.strip('-')
        return name
    
    def _read_secret(self, secret_name: str) -> Dict[str, str]:
        """Read a secret from Kubernetes and return decoded data."""
        # Normalize the secret name for K8s (underscores not allowed)
        k8s_secret_name = self._normalize_secret_name(secret_name)
        try:
            secret = self.api.read_namespaced_secret(name=k8s_secret_name, namespace=self.namespace)
            if secret.data is None:
                return {}
            # Decode base64-encoded secret data
            return {k: base64.b64decode(v).decode('utf-8') for k, v in secret.data.items()}
        except client.ApiException as e:
            if e.status == 404:
                raise Exception(f"Secret '{secret_name}' not found in namespace '{self.namespace}'")
            raise Exception(f"Failed to read secret '{secret_name}': {e.reason}")

    def getUserPassword(self, credential_name: str) -> Tuple[str, str]:
        """
        Read USER and PASSWORD from a Kubernetes secret.

        Expected secret format:
        - Key: USER -> value: username
        - Key: PASSWORD -> value: password

        Also accepts lowercase keys (user, password) for flexibility.
        """
        secret_data = self._read_secret(credential_name)
        user: str = secret_data.get('USER', '') or secret_data.get('user', '')
        password: str = secret_data.get('PASSWORD', '') or secret_data.get('password', '')

        if not user or not password:
            raise Exception(f"Secret '{credential_name}' missing required keys: USER, PASSWORD")

        return user, password

    def getPATSecret(self, credential_name: str) -> str:
        """
        Read a Personal Access Token from a Kubernetes secret.

        Expected secret format:
        - Key: token -> value: the PAT

        Also accepts uppercase (TOKEN) for flexibility.
        """
        secret_data = self._read_secret(credential_name)
        token: str = secret_data.get('token', '') or secret_data.get('TOKEN', '')

        if not token:
            raise Exception(f"Secret '{credential_name}' missing required key: token")

        return token

    def getApiKey(self, credential_name: str) -> Tuple[str, str]:
        """
        Read API key and secret from a Kubernetes secret.

        Expected secret format:
        - Key: api_key -> value: the API key
        - Key: api_secret -> value: the API secret

        Also accepts uppercase (API_KEY, API_SECRET) for flexibility.
        """
        secret_data = self._read_secret(credential_name)
        api_key: str = secret_data.get('api_key', '') or secret_data.get('API_KEY', '')
        api_secret: str = secret_data.get('api_secret', '') or secret_data.get('API_SECRET', '')

        if not api_key or not api_secret:
            raise Exception(f"Secret '{credential_name}' missing required keys: api_key, api_secret")

        return api_key, api_secret

    def getPrivateKeyAuth(self, credential_name: str) -> Tuple[str, str, str]:
        """
        Read username, private key, and passphrase from a Kubernetes secret.
        Used for key-pair authentication (e.g., Snowflake).

        Expected secret format:
        - Key: USER -> value: username
        - Key: PRIVATE_KEY -> value: PEM-encoded private key content
        - Key: PASSPHRASE -> value: passphrase for encrypted private key (can be empty)

        Also accepts lowercase keys for flexibility.
        """
        secret_data = self._read_secret(credential_name)
        user: str = secret_data.get('USER', '') or secret_data.get('user', '')
        private_key: str = secret_data.get('PRIVATE_KEY', '') or secret_data.get('private_key', '')
        passphrase: str = secret_data.get('PASSPHRASE', '') or secret_data.get('passphrase', '')

        if not user or not private_key:
            raise Exception(f"Secret '{credential_name}' missing required keys: USER, PRIVATE_KEY")

        return user, private_key, passphrase

secret_manager = K8sSecretManager(namespace='demo-cokub')


# Helper: build env vars for a credential given its type
def build_env_vars_for_credential(credential_name: str, credential_type: str) -> List[k8s.V1EnvVar]:
    """Return Kubernetes env vars for the given credential and type.
    Supports: USER_PASSWORD, API_KEY_PAIR, API_TOKEN, CLIENT_CERT_WITH_KEY.
    """
    if credential_type == 'USER_PASSWORD':
        user, password = secret_manager.getUserPassword(credential_name)
        envs = [
            SecretManager.to_envvar(f"{credential_name}_USER", user),
            SecretManager.to_envvar(f"{credential_name}_PASSWORD", password)
        ]
        envs.append(k8s.V1EnvVar(name=f"{credential_name}_TYPE", value='USER_PASSWORD'))
        return envs
    if credential_type == 'API_KEY_PAIR':
        api_key, api_secret = secret_manager.getApiKey(credential_name)
        envs = [
            SecretManager.to_envvar(f"{credential_name}_API_KEY", api_key),
            SecretManager.to_envvar(f"{credential_name}_API_SECRET", api_secret)
        ]
        envs.append(k8s.V1EnvVar(name=f"{credential_name}_TYPE", value='API_KEY_PAIR'))
        return envs
    if credential_type == 'API_TOKEN':
        token = secret_manager.getPATSecret(credential_name)
        envs = [SecretManager.to_envvar(f"{credential_name}_TOKEN", token)]
        envs.append(k8s.V1EnvVar(name=f"{credential_name}_TYPE", value='API_TOKEN'))
        return envs
    if credential_type == 'CLIENT_CERT_WITH_KEY':
        user, private_key, passphrase = secret_manager.getPrivateKeyAuth(credential_name)
        envs = [
            SecretManager.to_envvar(f"{credential_name}_USER", user),
            SecretManager.to_envvar(f"{credential_name}_PRIVATE_KEY", private_key),
            SecretManager.to_envvar(f"{credential_name}_PASSPHRASE", passphrase)
        ]
        envs.append(k8s.V1EnvVar(name=f"{credential_name}_TYPE", value='CLIENT_CERT_WITH_KEY'))
        return envs
    raise ValueError(f"Unsupported credential type '{credential_type}' for credential '{credential_name}'")


def build_otlp_env_vars(config: Dict, default_port: int = 4318) -> List[k8s.V1EnvVar]:
    """Build OpenTelemetry OTLP environment variables for node-local agent access.
    
    Uses Kubernetes Downward API to inject node IP, allowing pods to connect to
    DaemonSet-based telemetry agents (like Datadog) running on the same node.
    
    Args:
        config: Configuration dict that may contain 'otlp_enabled', 'otlp_port', 'otlp_protocol'
        default_port: Default OTLP port if not specified (default: 4318)
        
    Returns:
        List of environment variables for OTLP configuration, or empty list if not enabled
    """
    if not config.get('otlp_enabled'):
        return []
    
    env_vars = [
        # Inject node IP via Downward API for DaemonSet-based agents
        k8s.V1EnvVar(
            name='NODE_IP',
            value_from=k8s.V1EnvVarSource(field_ref=k8s.V1ObjectFieldSelector(field_path='status.hostIP'))
        ),
        k8s.V1EnvVar(name='OTEL_ENABLED', value='true'),
        k8s.V1EnvVar(
            name='OTEL_EXPORTER_OTLP_ENDPOINT',
            value=f'http://$(NODE_IP):{config.get("otlp_port", default_port)}'
        )
    ]
    
    # Add protocol if specified
    if config.get('otlp_protocol'):
        env_vars.append(k8s.V1EnvVar(name='OTEL_EXPORTER_OTLP_PROTOCOL', value=config['otlp_protocol']))
    
    return env_vars

# Strongly-typed configs for better readability and tooling
class PlatformConfig(TypedDict):
    platform_name: str
    original_platform_name: str
    namespace_name: str
    datasurface_docker_image: str
    merge_db_credential_secret_name: str
    merge_db_credential_secret_type: str
    merge_db_driver: str
    merge_db_hostname: str
    merge_db_port: int
    merge_db_database: str
    merge_db_query: NotRequired[str]
    git_credential_secret_name: str
    git_credential_secret_type: str
    git_repo_type: str
    git_repo_url: str
    git_repo_owner: str
    git_repo_repo_name: str
    git_repo_branch: str
    git_credential_name: str
    git_cache_max_age_minutes: int
    git_cache_enabled: NotRequired[bool]
    rte_name: NotRequired[str]
    phys_dag_table_name: str
    phys_datatransformer_table_name: str
    otlp_enabled: NotRequired[bool]
    otlp_port: NotRequired[int]
    otlp_protocol: NotRequired[str]


class StreamConfig(TypedDict):
    stream_key: str
    schedule_string: str
    trigger_config: NotRequired[Dict]
    store_name: str
    ingestion_type: NotRequired[str]
    source_credential_secret_name: NotRequired[str]
    source_credential_secret_type: NotRequired[str]
    dataset_name: NotRequired[str]
    job_limits: NotRequired[Dict[str, str]]
    priority: NotRequired[int]
    tags: NotRequired[List[str]]


class DataTransformerConfig(TypedDict):
    workspace_name: str
    output_datastore_name: str
    input_dag_ids: NotRequired[List[str]]
    schedule_string: NotRequired[str]
    job_limits: NotRequired[Dict[str, str]]
    output_job_limits: NotRequired[Dict[str, str]]
    priority: NotRequired[int]
    git_credential_secret_name: NotRequired[str]
    git_credential_secret_type: NotRequired[str]
    dt_credential_secret_name: NotRequired[str]
    dt_credential_secret_type: NotRequired[str]
    tags: NotRequired[List[str]]


# Default arguments for the DAG
default_args = {
    'owner': 'datasurface',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  # No retries for infrastructure tasks - validation errors should fail immediately
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'demo-psp_infrastructure',
    default_args=default_args,
    description='Infrastructure DAG for demo-psp Data Platform',
    schedule='*/5 * * * *',  # Run every 5 minutes to pick up model changes quickly
    catchup=False,
    max_active_runs=1,
    tags=['datasurface', 'infrastructure', 'demo-psp'],
    is_paused_upon_creation=False  # Start unpaused so DAG is immediately active
)

# Start task
start_task = EmptyOperator(
    task_id='start_infrastructure_tasks',
    dag=dag
)

# Environment variables for all tasks
common_env_vars = [
    # Platform configuration (literal values)
    k8s.V1EnvVar(name='DATASURFACE_PSP_NAME', value='demo-psp'),
    k8s.V1EnvVar(name='DATASURFACE_NAMESPACE', value='demo-cokub'),
]

# Merge and Git credentials (typed)
common_env_vars.extend(build_env_vars_for_credential(
    'postgres-demo-merge',
    'USER_PASSWORD'
))
common_env_vars.extend(build_env_vars_for_credential(
    'git',
    'API_TOKEN'
))



# OpenTelemetry OTLP configuration for node-local agent access
common_env_vars.extend(build_otlp_env_vars({
    'otlp_enabled': False,
    'otlp_port': 4318,
    
}))


# MERGE Task - Generates infrastructure terraform files using model merge handler
merge_task = KubernetesPodOperator(
    task_id='infrastructure_merge_task',
    name='demo-psp-infra-merge',
    namespace='demo-cokub',
    image='registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.2.2',
    cmds=['/bin/bash'],
    arguments=[
        '-c',
        '''
        echo "🔄 Starting DataSurface Infrastructure Model Merge Handler"

        # Set PYTHONPATH for DataSurface modules
        export PYTHONPATH="/app/src"

        # Run merge handler for platform using cache-aware CLI
        echo "🔧 Running infrastructure model merge handler with shared cache..."
        
        # Capture both stdout and stderr to make validation errors visible in Airflow logs
        if python -m datasurface.cmd.platform handleModelMerge \\
          --git-repo-path "/cache/git-cache" \\
          --git-repo-type "github" \\
          --git-repo-url "https://github.com" \\
          --git-repo-owner "billynewport" \\
          --git-repo-name "demo_cokub_model" \\
          --git-repo-branch "main" \\
          --git-platform-repo-credential-name "git" \\
          --git-release-selector-hex 7b2274797065223a202256657273696f6e5061747465726e52656c6561736553656c6563746f72222c202276657273696f6e5061747465726e223a2022765b302d395d2b5c5c2e5b302d395d2b5c5c2e5b302d395d2b2d64656d6f222c202272656c6561736554797065223a2022737461626c655f6f6e6c79227d \\
          --use-git-cache \\
          --rte-name "demo" \\
          --max-cache-age-minutes "5" \\
          --output "/workspace/generated_artifacts" \\
          --psp "Demo_PSP" 2>&1; then
            echo "✅ Infrastructure model merge handler complete!"
        else
            echo ""
            echo "❌❌❌ INFRASTRUCTURE MODEL MERGE FAILED ❌❌❌"
            echo "The model validation found errors that prevent the infrastructure from being deployed."
            echo "Please check the validation errors above and fix the model before retrying."
            echo "Common issues:"
            echo "  - Missing platformMD configuration"
            echo "  - Invalid DataPlatformManagedDataContainer references"
            echo "  - Model constraint violations"
            echo "❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌"
            exit 1
        fi
        '''
    ],
    env_vars=common_env_vars,  # type: ignore
    
    volumes=[
        k8s.V1Volume(
            name='git-model-cache',
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name='git-cache-pvc'
            )
        ),
        k8s.V1Volume(
            name='workspace',
            empty_dir=k8s.V1EmptyDirVolumeSource()
        )
    ],
    volume_mounts=[
        k8s.V1VolumeMount(
            name='git-model-cache',
            mount_path='/cache/git-cache',
            read_only=False
        ),
        k8s.V1VolumeMount(
            name='workspace',
            mount_path='/workspace',
            read_only=False
        )
    ],
    
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# Reconcile Views Task removed - now handled by DC-level reconcile DAGs (every 15 minutes)
# Legacy reconcile_views_task is replaced by dedicated per-DataContainer reconcile DAGs
# See load_dc_reconcile_configurations() for the new approach

# Metrics Collector Task - Placeholder for future metrics collection
metrics_collector_task = EmptyOperator(
    task_id='metrics_collector_task',
    dag=dag
)

# Apply Security Task - Placeholder for future security operations
apply_security_task = EmptyOperator(
    task_id='apply_security_task',
    dag=dag
)

# Table Removal Task - Placeholder for future table cleanup operations
table_removal_task = EmptyOperator(
    task_id='table_removal_task',
    dag=dag
)

# Copy the exact factory DAG creation logic from the original templates
# This is identical to yellow_platform_factory_dag.py.j2 functionality

def create_ingestion_stream_dag(platform_config: PlatformConfig, stream_config: StreamConfig) -> DAG:
    """Create a single ingestion stream DAG from configuration - identical to original template"""
    platform_name = platform_config['platform_name']
    stream_key = stream_config['stream_key']
    schedule_string = stream_config['schedule_string']
    trigger_config = stream_config.get('trigger_config', {})
    tag_list = stream_config.get('tags', [])

    # Default arguments for the DAG
    default_args = INGESTION_DAG_DEFAULT_ARGS

    def _default_truthy(v):
        return v not in (0, '0', '', None, False)

    def _eval_success_condition(value, condition):
        if condition is None or str(condition).strip() == '':
            return _default_truthy(value)
        cond = str(condition).strip()
        for op in ('>=', '<=', '==', '!=', '>', '<'):
            if cond.startswith(op):
                rhs_raw = cond[len(op):].strip()
                try:
                    if rhs_raw.startswith("'") and rhs_raw.endswith("'"):
                        rhs = rhs_raw[1:-1]
                    elif rhs_raw.startswith('"') and rhs_raw.endswith('"'):
                        rhs = rhs_raw[1:-1]
                    elif '.' in rhs_raw:
                        rhs = float(rhs_raw)
                    else:
                        rhs = int(rhs_raw)
                except ValueError:
                    rhs = rhs_raw
                if op == '>=':
                    return value >= rhs
                if op == '<=':
                    return value <= rhs
                if op == '==':
                    return value == rhs
                if op == '!=':
                    return value != rhs
                if op == '>':
                    return value > rhs
                if op == '<':
                    return value < rhs
        return _default_truthy(value)

    def _check_sensor_trigger():
        tcfg = trigger_config or {}
        ttype = tcfg.get('type')
        if ttype == 'FileExistsTrigger':
            filepath = tcfg.get('filepath')
            return bool(filepath and os.path.exists(filepath))
        if ttype == 'HttpReadyTrigger':
            endpoint = tcfg.get('endpoint')
            method = tcfg.get('method', 'GET')
            headers = tcfg.get('headers') or {}
            expected = int(tcfg.get('expected_status', 200))
            response_check = tcfg.get('response_check')
            if not endpoint:
                return False
            req = urllib.request.Request(endpoint, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    if resp.status != expected:
                        return False
                    if response_check:
                        body = resp.read().decode('utf-8', errors='replace')
                        if str(response_check).startswith('re:'):
                            return re.search(str(response_check)[3:], body) is not None
                        return str(response_check) in body
                    return True
            except urllib.error.URLError:
                return False
        if ttype == 'SqlConditionTrigger':
            cred_name = tcfg.get('credential_secret_name')
            if not cred_name:
                return False
            user = os.getenv(f"{cred_name}_USER")
            password = os.getenv(f"{cred_name}_PASSWORD")
            if not user or not password:
                return False
            engine = create_database_connection(
                tcfg.get('db_driver'),
                user,
                password,
                tcfg.get('db_hostname'),
                int(tcfg.get('db_port')),
                tcfg.get('db_database'),
                tcfg.get('db_query')
            )
            try:
                with engine.begin() as conn:
                    result = conn.execute(text(tcfg.get('sql')))
                    row = result.fetchone()
                    first_cell = row[0] if row is not None and len(row) > 0 else None
                    return _eval_success_condition(first_cell, tcfg.get('success_condition'))
            finally:
                engine.dispose()
        if ttype == 'CloudObjectTrigger':
            local_base = tcfg.get('local_base_path')
            key = tcfg.get('key')
            if local_base and key:
                return os.path.exists(os.path.join(local_base, key))
            bucket = tcfg.get('bucket')
            if not bucket or not key:
                return False
            try:
                import boto3
                s3_kwargs = {}
                if tcfg.get('aws_region'):
                    s3_kwargs['region_name'] = tcfg.get('aws_region')
                if tcfg.get('s3_endpoint_url'):
                    s3_kwargs['endpoint_url'] = tcfg.get('s3_endpoint_url')
                cred_name = tcfg.get('credential_secret_name')
                if cred_name:
                    access_key = os.getenv(f"{cred_name}_API_KEY")
                    secret_key = os.getenv(f"{cred_name}_API_SECRET")
                    if access_key and secret_key:
                        s3_kwargs['aws_access_key_id'] = access_key
                        s3_kwargs['aws_secret_access_key'] = secret_key
                client_s3 = boto3.client('s3', **s3_kwargs)
                client_s3.head_object(Bucket=bucket, Key=key)
                return True
            except Exception:
                return False
        return True

    # Create the DAG
    dag = DAG(
        f'{platform_name}__{stream_key}_ingestion',
        default_args=default_args,
        description=f'Ingestion Stream DAG for {platform_name}__{stream_key}',
        schedule=schedule_string,  # External trigger schedule
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=False,  # Start unpaused so DAGs are immediately active
        tags=tag_list
    )

    # Environment variables for the job - combining literal and secret-based vars
    env_vars = build_common_env_vars(platform_config)

    # Source database credentials for SQL snapshot ingestion (only if different from merge store)
    maybe_add_source_db_env(env_vars, platform_config, stream_config)
    # Trigger-specific credential injection for sensor execution if needed
    t_cred_name = trigger_config.get('credential_secret_name')
    t_cred_type = trigger_config.get('credential_secret_type')
    if t_cred_name and t_cred_type:
        env_vars.extend(build_env_vars_for_credential(t_cred_name, t_cred_type))

    # Function to determine next action based on job return code
    def determine_next_action(**context):
        """Decide next action using shared log parsing (strict mode for ingestion)."""
        dag_run = context['dag_run']
        # Determine the correct task name based on DAG type
        task_name = "output_ingestion_job" if "_dt_ingestion" in dag_run.dag_id else "snapshot_merge_job"
        content = read_latest_task_log(dag_run.dag_id, dag_run.run_id, task_name)
        result = choose_next_action_from_log(content, strict=True)
        
        # Airflow 3.x: Explicitly push XCom to ensure downstream tasks can detect success
        # BranchPythonOperator auto-push may not work reliably in Airflow 3.x SDK
        ti = context['ti']
        ti.xcom_push(key='branch_success', value=True)
        
        return result

    # Job task
    job_args = [
        '--platform-name', platform_config['original_platform_name'],
        '--store-name', stream_config['store_name'],
        '--operation', 'snapshot-merge',
        '--git-repo-path', '/cache/git-cache',  # Use cache mount path
        '--git-repo-type', platform_config['git_repo_type'],
        '--git-repo-url', platform_config['git_repo_url'],
        '--git-repo-owner', platform_config['git_repo_owner'],
        '--git-repo-name', platform_config['git_repo_repo_name'],
        '--git-repo-branch', platform_config['git_repo_branch'],
        '--git-platform-repo-credential-name', platform_config['git_credential_name'],
        '--rte-name', 'demo',
        '--max-cache-age-minutes', str(platform_config['git_cache_max_age_minutes'])  # Cache freshness threshold
    ]

    # Add release selector if provided (hex-encoded, no quoting issues)
    if platform_config.get('git_release_selector'):
        job_args.extend(['--git-release-selector-hex', platform_config['git_release_selector']])

    if platform_config.get('git_cache_enabled'):
        job_args.append('--use-git-cache')  # Enable cache usage

    # Add dataset name if present
    if stream_config.get('dataset_name'):
        job_args.extend(['--dataset-name', stream_config['dataset_name']])

    # Git cache mounts
    _vols, _mounts = git_cache_mounts(platform_config.get('git_cache_enabled'))

    job = build_pod_operator(
        task_id='snapshot_merge_job',
        name=f"{platform_name}-{stream_key.replace('-', '_')}-job",
        namespace=platform_config['namespace_name'],
        image=platform_config['datasurface_docker_image'],
        cmds=['python', '-m', 'datasurface.platforms.yellow.jobs'],
        arguments=job_args,
        env_vars=env_vars,
        image_pull_policy='IfNotPresent',
        volumes=_vols,
        volume_mounts=_mounts,
        resources=pod_resources_from_limits(
            stream_config.get('job_limits', {}),
            {'requested_memory': '256Mi', 'requested_cpu': '100m', 'limits_memory': '256Mi', 'limits_cpu': '100m'}
        ),
        dag=dag,
        priority_weight=stream_config.get('priority', 0),
        weight_rule='absolute',
        do_xcom_push=False,
    )

    # Sensor gate for condition-based triggers
    sensor_task = None
    ttype = trigger_config.get('type')
    if ttype in ('SqlConditionTrigger', 'FileExistsTrigger', 'HttpReadyTrigger', 'CloudObjectTrigger'):
        sensor_task = PythonSensor(
            task_id='wait_for_sensor_trigger',
            python_callable=_check_sensor_trigger,
            mode=trigger_config.get('mode', 'reschedule'),
            timeout=int(trigger_config.get('timeout', 3600)),
            poke_interval=int(trigger_config.get('poke_interval', 60)),
            dag=dag
        )

    # Branch operator
    branch = BranchPythonOperator(
        task_id='check_result',
        python_callable=determine_next_action,
        dag=dag,
        trigger_rule='all_success'  # Only run if all upstream tasks succeed - failures should fail the DAG
    )

    # Reschedule immediately
    reschedule = TriggerDagRunOperator(
        task_id='reschedule_immediately',
        trigger_dag_id=f'{platform_name}__{stream_key}_ingestion',  # Trigger the SAME DAG
        conf={'triggered_by': 'reschedule'},
        dag=dag
    )

    # Wait for trigger
    wait = EmptyOperator(
        task_id='wait_for_trigger',
        dag=dag
    )

    # Cleanup task - check for failures and propagate to fail the DAG run
    # Airflow 3.x Bug Workaround: DAG runs show as success if final leaf task succeeds,
    # even when upstream tasks failed. We must explicitly check and raise an exception.
    def cleanup_and_check_failures(**context):
        # Airflow 3.x: Check if branch task (check_result) ran successfully
        # We use an explicit XCom key 'branch_success' pushed by determine_next_action
        ti = context['ti']
        
        # Check for our explicit success marker pushed by the branch task
        branch_success = ti.xcom_pull(task_ids='check_result', key='branch_success')
        
        if branch_success is True:
            # Branch ran successfully - this means snapshot_merge_job succeeded
            return "Cleanup complete - DAG succeeded"
        
        # Branch didn't run or didn't push success marker - critical tasks must have failed
        raise Exception(
            "Ingestion DAG failed - snapshot_merge_job did not complete successfully. "
            "Check task logs for details."
        )

    # Airflow 3.x: This task runs after all others complete and checks for failures
    cleanup_task = PythonOperator(
        task_id='cleanup_ingestion',
        python_callable=cleanup_and_check_failures,
        dag=dag,
        trigger_rule='all_done'
    )

    # Set up dependencies
    if sensor_task:
        sensor_task >> job
    job >> branch
    branch >> [reschedule, wait]
    [reschedule, wait] >> cleanup_task

    return dag

def create_database_connection(driver: str, user: str, password: str, host: str, port: int, database: str, query_driver: Optional[str] = None):
    """Helper function to create database connection URL and engine consistently"""
    # Build common URL parameters
    url_params = {
        "drivername": driver,
        "username": user,
        "password": password,
        "host": host,
        "port": port,
        "database": database,
    }

    # Add query parameters if needed
    if query_driver:
        url_params["query"] = {"driver": query_driver}

    db_url = URL.create(**url_params)
    return create_engine(db_url)


def fetch_rows(engine, sql: str):
    """Read rows using a simple text SQL statement within a transaction."""
    with engine.begin() as connection:
        result = connection.execute(text(sql))
        return list(result.fetchall())


def map_rows_to_dags(rows, row_to_id_and_dag):
    """Map database rows to a dict of {dag_id: dag_object}.
    
    Individual row failures (e.g., missing K8s secret) are logged and skipped,
    allowing other DAGs to continue loading. This prevents one misconfigured
    DAG from breaking all DAGs of that type.
    """
    desired = {}
    for row in rows:
        try:
            dag_id, dag_obj = row_to_id_and_dag(row)
            desired[dag_id] = dag_obj
        except Exception as e:
            # Log the error but continue processing other rows
            # row[0] is typically the dag_id or primary key
            row_id = row[0] if row else 'unknown'
            print(f"⚠️  Error creating DAG for row '{row_id}': {e}")
            _logger.error(f"Failed to create DAG for row '{row_id}': {e}")
    return desired


def sync_dag_lifecycle(existing_dags: dict, desired_dags: dict, logger):
    """Create/update/remove DAGs in globals() to match desired set, with logging."""
    existing_ids = set(existing_dags.keys())
    desired_ids = set(desired_dags.keys())
    to_create = desired_ids - existing_ids
    to_remove = existing_ids - desired_ids
    to_update = desired_ids & existing_ids

    removed, created, updated = [], [], []

    if to_remove:
        logger("🗑️  REMOVING OBSOLETE DAGs:")
        for dag_id in sorted(to_remove):
            if dag_id in globals():
                del globals()[dag_id]
                removed.append(dag_id)
                logger(f"   🗑️  REMOVED: {dag_id}")

    if to_create:
        logger("🆕 CREATING NEW DAGs:")
        for dag_id in sorted(to_create):
            globals()[dag_id] = desired_dags[dag_id]
            created.append(dag_id)
            logger(f"   ✅ CREATED: {dag_id}")

    if to_update:
        logger("🔄 UPDATING EXISTING DAGs:")
        for dag_id in sorted(to_update):
            globals()[dag_id] = desired_dags[dag_id]
            updated.append(dag_id)
            logger(f"   🔄 UPDATED: {dag_id}")

    return f"-{len(removed)} +{len(created)} ~{len(updated)} = {len(desired_dags)}"


def build_merge_engine_from_env(config: PlatformConfig):
    """Create a DB engine from merge config using secret_manager. Returns engine or None if creds missing."""
    try:
        user, password = secret_manager.getUserPassword(config['merge_db_credential_secret_name'])
    except Exception:
        return None
    return create_database_connection(
        driver=config['merge_db_driver'],
        user=user,
        password=password,
        host=config['merge_db_hostname'],
        port=config['merge_db_port'],
        database=config['merge_db_database'],
        query_driver=config.get('merge_db_query', None)
    )


def build_merge_engine_from_env_template(credential_secret_name: str, credential_secret_type: str, driver: str, host: str, port: int, database: str, query_driver):
    """Top-level helper for template-driven params. Builds and returns a SQLAlchemy engine.
    Raises Exception on failure or unsupported credential type.
    Supported types: USER_PASSWORD.
    """
    # Normalize query_driver coming from template (could be 'None' string)
    qd = None if (query_driver in (None, '', 'None')) else query_driver
    if credential_secret_type == 'USER_PASSWORD':
        user, password = secret_manager.getUserPassword(credential_secret_name)
        return create_database_connection(
            driver=driver,
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            query_driver=qd
        )
    # API_TOKEN and API_KEY_PAIR are not applicable for SQLAlchemy DB auth here
    raise Exception(f"Unsupported credential type for merge DB engine: {credential_secret_type}")


def build_common_env_vars(platform_config: PlatformConfig):
    """Common env vars shared by ingestion and datatransformer jobs."""
    base = [
        k8s.V1EnvVar(name='DATASURFACE_PLATFORM_NAME', value=platform_config['original_platform_name']),
        k8s.V1EnvVar(name='DATASURFACE_NAMESPACE', value=platform_config['namespace_name'])
    ]
    # Merge credential
    base.extend(build_env_vars_for_credential(
        platform_config['merge_db_credential_secret_name'],
        platform_config['merge_db_credential_secret_type']
    ))
    # Git credential
    base.extend(build_env_vars_for_credential(
        platform_config['git_credential_secret_name'],
        platform_config['git_credential_secret_type']
    ))
    # Event publish credential (for Kafka event publishing)
    if platform_config.get('event_publish_credential_secret_name'):
        base.extend(build_env_vars_for_credential(
            platform_config['event_publish_credential_secret_name'],
            platform_config.get('event_publish_credential_secret_type', 'USER_PASSWORD')
        ))
    # OpenTelemetry OTLP configuration
    base.extend(build_otlp_env_vars(platform_config))
    return base


def maybe_add_source_db_env(env_vars, platform_config: PlatformConfig, stream_config: StreamConfig):
    """Add source DB USER/PASSWORD if sql_source with different credential from merge store."""
    if (stream_config.get('ingestion_type') == 'sql_source' and
        stream_config.get('source_credential_secret_name') and
        stream_config['source_credential_secret_name'] != platform_config['merge_db_credential_secret_name']):
        env_vars.extend(build_env_vars_for_credential(
            stream_config['source_credential_secret_name'],
            stream_config.get('source_credential_secret_type', 'USER_PASSWORD')
        ))



def git_cache_mounts(enabled: bool):
    """Return (volumes, mounts) for git cache PVC, or ([], []) if disabled."""
    if not enabled:
        return [], []
    volumes = [
        k8s.V1Volume(
            name='git-model-cache',
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name='git-cache-pvc'
            )
        )
    ]
    mounts = [
        k8s.V1VolumeMount(
            name='git-model-cache',
            mount_path='/cache/git-cache',
            read_only=False
        )
    ]
    return volumes, mounts


def pod_resources_from_limits(limits: dict, defaults: dict):
    """Create Kubernetes resource requirements from limits with fallback defaults."""
    req_mem = limits.get('requested_memory', defaults['requested_memory'])
    req_cpu = limits.get('requested_cpu', defaults['requested_cpu'])
    lim_mem = limits.get('limits_memory', defaults['limits_memory'])
    lim_cpu = limits.get('limits_cpu', defaults['limits_cpu'])
    return k8s.V1ResourceRequirements(
        requests={'memory': req_mem, 'cpu': req_cpu},
        limits={'memory': lim_mem, 'cpu': lim_cpu}
    )


def build_pod_operator(
    *,
    task_id: str,
    name: str,
    namespace: str,
    image: str,
    cmds,
    arguments,
    env_vars,
    image_pull_policy: Optional[str],
    volumes,
    volume_mounts,
    resources,
    dag: DAG,
    priority_weight: int = 0,
    weight_rule: str = 'absolute',
    trigger_rule: Optional[str] = None,
    do_xcom_push: bool = False,
):
    """Create a KubernetesPodOperator with shared defaults applied.
    
    Airflow 3.x Notes:
    - is_delete_operator_pod is deprecated, use on_finish_action instead
    - on_finish_action="delete_succeeded_pod" keeps failed pods for debugging
    - log_events_on_failure=True captures K8s events when pod fails
    """
    kwargs = {
        'task_id': task_id,
        'name': name,
        'namespace': namespace,
        'image': image,
        'cmds': cmds,
        'arguments': arguments,
        'env_vars': env_vars,  # type: ignore
        'image_pull_policy': image_pull_policy,
        'volumes': volumes,
        'volume_mounts': volume_mounts,
        'get_logs': True,
        'do_xcom_push': do_xcom_push,
        'container_resources': resources,
        # Airflow 3.x: Use on_finish_action instead of deprecated is_delete_operator_pod
        # delete_succeeded_pod: keeps failed pods for debugging, deletes successful ones
        'on_finish_action': "delete_succeeded_pod",
        # Log K8s events when pod fails - helps debug container startup issues
        'log_events_on_failure': True,
        'dag': dag,
        'priority_weight': priority_weight,
        'weight_rule': weight_rule,
    }
    
    # Only add trigger_rule if it's not None
    if trigger_rule is not None:
        kwargs['trigger_rule'] = trigger_rule
    
    return KubernetesPodOperator(**kwargs)

# Task log helpers used by determine_next_action in both DAG types
def read_latest_task_log(dag_id: str, run_id: str, task_id: str):
    """Return the content of the latest attempt log for a given task, or None if not found."""
    import os
    log_dir = f"/opt/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}"
    if not os.path.exists(log_dir):
        return None
    try:
        attempt_files = [f for f in os.listdir(log_dir) if f.startswith('attempt=') and f.endswith('.log')]
        log_file = None
        if attempt_files:
            log_file = os.path.join(log_dir, max(attempt_files))
        else:
            # Fallback to attempt=1.log if present
            fallback = os.path.join(log_dir, 'attempt=1.log')
            if os.path.exists(fallback):
                log_file = fallback
        if log_file and os.path.exists(log_file):
            with open(log_file, 'r') as f:
                return f.read()
    except Exception:
        return None
    return None


def choose_next_action_from_log(content, strict: bool):
    """Decide next action from log content.
    - If DATASURFACE_RESULT_CODE is present: 1->reschedule, 0->wait, else error (strict) or wait.
    - Else, fall back to keywords used by DataTransformer jobs.
    - If nothing conclusive: error (strict) or wait.
    """
    import re
    if content:
        match = re.search(r'DATASURFACE_RESULT_CODE=(-?\d+)', content)
        if match:
            code = int(match.group(1))
            if code == 1:
                return 'reschedule_immediately'
            if code == 0:
                return 'wait_for_trigger'
            if strict:
                raise Exception(f"SnapshotMergeJob failed with code {code} - manual intervention required")
            return 'wait_for_trigger'
        # Fallback keyword checks
        if "RESCHEDULE_IMMEDIATELY" in content:
            return 'reschedule_immediately'
        if "JOB_COMPLETED_SUCCESSFULLY" in content:
            return 'wait_for_trigger'
    # No content or no recognizable markers
    if strict:
        raise Exception("No result code found in logs")
    return 'wait_for_trigger'




# Merge env var helpers (for clearer messages and consistency)
def get_merge_env_names(config: PlatformConfig):
    """Return the expected env var names for merge DB credentials (config-driven)."""
    base = config['merge_db_credential_secret_name']
    return f"{base}_USER", f"{base}_PASSWORD"


def get_merge_env_names_template(credential_secret_name: str):
    """Return expected env var names for merge DB credentials (template-driven).
    
    The credential_secret_name is already K8s-normalized (lowercase, hyphens).
    Env vars use this name directly with _USER/_PASSWORD suffixes.
    """
    return f"{credential_secret_name}_USER", f"{credential_secret_name}_PASSWORD"


# Default args for dynamically created DAGs
INGESTION_DAG_DEFAULT_ARGS = {
    'owner': 'datasurface',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# DAG type constants for parsing
# WARNING: These values MUST match the DAGIDSuffix enum in yellow_dp.py
# If you change these, you MUST also update the enum in:
#   src/datasurface/platforms/yellow/yellow_dp.py::DAGIDSuffix
DAG_TYPE_INFRASTRUCTURE = 'infrastructure'
DAG_TYPE_INGESTION = 'ingestion'
DAG_TYPE_DATATRANSFORMER = 'datatransformer'
DAG_TYPE_FACTORY = 'factory'
DAG_TYPE_DT_FACTORY = 'dt_factory'
DAG_TYPE_CQRS = 'cqrs'

# DAG ID suffixes - MUST match DAGIDSuffix enum in yellow_dp.py
DAG_SUFFIX_INFRASTRUCTURE = '_infrastructure'
DAG_SUFFIX_FACTORY = '_factory_dag'
DAG_SUFFIX_DT_FACTORY = '_datatransformer_factory'
DAG_SUFFIX_INGESTION = '_ingestion'
DAG_SUFFIX_DT_INGESTION = '_dt_ingestion'
DAG_SUFFIX_DATATRANSFORMER = '_datatransformer'
DAG_SUFFIX_CQRS = '_cqrs'
DAG_PLATFORM_SEPARATOR = '__'


def _parse_dag_id(dag_id: str) -> dict:
    """Parse a dag_id to determine its type and extract components.
    
    Returns a dict with:
        - 'type': One of DAG_TYPE_* constants
        - 'platform': Platform name (if applicable)
        - 'stream_key': Stream key (for ingestion DAGs)
        - 'workspace': Workspace name (for datatransformer DAGs)
        - 'psp': PSP name (for CQRS DAGs)
        - 'crg': CRG name (for CQRS DAGs)
        - 'dc': Data container name (for CQRS DAGs)
    """
    result = {'type': None, 'dag_id': dag_id}
    
    # Infrastructure DAG: {psp_k8s_name}_infrastructure
    if dag_id.endswith('_infrastructure'):
        result['type'] = DAG_TYPE_INFRASTRUCTURE
        result['psp_k8s_name'] = dag_id[:-15]  # Remove '_infrastructure'
        return result
    
    # Factory DAG: {platform}_factory_dag
    if dag_id.endswith('_factory_dag'):
        result['type'] = DAG_TYPE_FACTORY
        result['platform'] = dag_id[:-12]  # Remove '_factory_dag'
        return result
    
    # DataTransformer Factory DAG: {platform}_datatransformer_factory
    if dag_id.endswith('_datatransformer_factory'):
        result['type'] = DAG_TYPE_DT_FACTORY
        result['platform'] = dag_id[:-24]  # Remove '_datatransformer_factory'
        return result
    
    # CQRS DAG: {psp}_{crg}_{dc}_cqrs
    if dag_id.endswith('_cqrs'):
        result['type'] = DAG_TYPE_CQRS
        # CQRS format: psp_crg_dc_cqrs - need to parse carefully
        # The PSP, CRG, and DC names can contain underscores, so we need DB lookup
        result['cqrs_prefix'] = dag_id[:-5]  # Remove '_cqrs'
        return result
    
    # Ingestion DAG: {platform}__{stream_key}_ingestion
    if dag_id.endswith('_ingestion') and '__' in dag_id:
        result['type'] = DAG_TYPE_INGESTION
        # Split on double underscore to separate platform from stream_key
        prefix = dag_id[:-10]  # Remove '_ingestion'
        parts = prefix.split('__', 1)
        if len(parts) == 2:
            result['platform'] = parts[0]
            result['stream_key'] = parts[1]
        return result
    
    # DataTransformer DAG: {platform}__{workspace}_datatransformer
    if dag_id.endswith('_datatransformer') and '__' in dag_id:
        result['type'] = DAG_TYPE_DATATRANSFORMER
        # Split on double underscore to separate platform from workspace
        prefix = dag_id[:-16]  # Remove '_datatransformer'
        parts = prefix.split('__', 1)
        if len(parts) == 2:
            result['platform'] = parts[0]
            result['workspace'] = parts[1]
        return result
    
    return result  # Unknown type


def _create_single_ingestion_dag(dag_id: str, platform: str) -> Optional[DAG]:
    """Create a single ingestion DAG by querying just that record from the database using dag_id as PK."""
    try:
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host='postgres-co',
            port=5432,
            database='merge_db',
            query_driver=''
        )
        if engine is None:
            return None
        
        try:
            # Query for the platform's factory config to get the ingestion table name
            factory_table_name = 'demo_psp_factory_dags'
            factory_dag_id = f"{platform}_factory_dag"
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT config_json FROM {factory_table_name}
                    WHERE dag_id = :dag_id AND status = 'active'
                """), {'dag_id': factory_dag_id})
                factory_row = result.fetchone()
                
            if not factory_row:
                return None
            
            platform_config = json.loads(factory_row[0])
            
            # Query for the specific ingestion stream by dag_id (direct lookup)
            dag_table = platform_config['phys_dag_table_name']
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT config_json FROM {dag_table}
                    WHERE dag_id = :dag_id AND status = 'active'
                """), {'dag_id': dag_id})
                stream_row = result.fetchone()
                
            if not stream_row:
                return None
            
            stream_config = json.loads(stream_row[0])
            return create_ingestion_stream_dag(platform_config, stream_config)
        finally:
            engine.dispose()
    except Exception as e:
        _logger.warning(f"Failed to create single ingestion DAG {dag_id}: {e}")
        return None


def _create_single_datatransformer_dag(dag_id: str, platform: str) -> Optional[DAG]:
    """Create a single datatransformer DAG by querying just that record from the database using dag_id as PK."""
    try:
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host='postgres-co',
            port=5432,
            database='merge_db',
            query_driver=''
        )
        if engine is None:
            return None
        
        try:
            # Query for the platform's factory config to get the datatransformer table name
            factory_table_name = 'demo_psp_factory_dags'
            factory_dag_id = f"{platform}_datatransformer_factory"
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT config_json FROM {factory_table_name}
                    WHERE dag_id = :dag_id AND status = 'active'
                """), {'dag_id': factory_dag_id})
                factory_row = result.fetchone()
                
            if not factory_row:
                return None
            
            platform_config = json.loads(factory_row[0])
            
            # Query for the specific datatransformer by dag_id (direct lookup)
            dt_table = platform_config['phys_datatransformer_table_name']
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT config_json FROM {dt_table}
                    WHERE dag_id = :dag_id AND status = 'active'
                """), {'dag_id': dag_id})
                dt_row = result.fetchone()
                
            if not dt_row:
                return None
            
            dt_config = json.loads(dt_row[0])
            return create_datatransformer_execution_dag(platform_config, dt_config)
        finally:
            engine.dispose()
    except Exception as e:
        _logger.warning(f"Failed to create single datatransformer DAG {dag_id}: {e}")
        return None


def _create_single_cqrs_dag(dag_id: str) -> Optional[DAG]:
    """Create a single CQRS DAG by direct lookup using dag_id as primary key."""
    try:
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host='postgres-co',
            port=5432,
            database='merge_db',
            query_driver=''
        )
        if engine is None:
            return None

        try:
            # Direct lookup by dag_id (O(1) indexed lookup)
            cqrs_table_name = 'demo_psp_cqrs_dags'
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT config_json FROM {cqrs_table_name}
                    WHERE dag_id = :dag_id AND status = 'active'
                """), {'dag_id': dag_id})
                row = result.fetchone()

            if row:
                config = json.loads(row[0])
                return create_cqrs_execution_dag(config)

            return None
        finally:
            engine.dispose()
    except Exception as e:
        _logger.warning(f"Failed to create single CQRS DAG {dag_id}: {e}")
        return None


def _create_single_factory_dag(dag_id: str) -> Optional[DAG]:
    """Create a single platform factory DAG by querying just that record from the database using dag_id as PK."""
    try:
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host='postgres-co',
            port=5432,
            database='merge_db',
            query_driver=''
        )
        if engine is None:
            return None
        
        try:
            factory_table_name = 'demo_psp_factory_dags'
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT config_json FROM {factory_table_name}
                    WHERE dag_id = :dag_id AND status = 'active'
                """), {'dag_id': dag_id})
                row = result.fetchone()
                
            if not row:
                return None
            
            config = json.loads(row[0])
            return create_platform_factory_dag(config)
        finally:
            engine.dispose()
    except Exception as e:
        _logger.warning(f"Failed to create single factory DAG {dag_id}: {e}")
        return None


def _create_single_dt_factory_dag(dag_id: str) -> Optional[DAG]:
    """Create a single datatransformer factory DAG by querying just that record from the database using dag_id as PK."""
    try:
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host='postgres-co',
            port=5432,
            database='merge_db',
            query_driver=''
        )
        if engine is None:
            return None
        
        try:
            factory_table_name = 'demo_psp_factory_dags'
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT config_json FROM {factory_table_name}
                    WHERE dag_id = :dag_id AND status = 'active'
                """), {'dag_id': dag_id})
                row = result.fetchone()
                
            if not row:
                return None
            
            config = json.loads(row[0])
            return create_datatransformer_factory_dag(config)
        finally:
            engine.dispose()
    except Exception as e:
        _logger.warning(f"Failed to create single DT factory DAG {dag_id}: {e}")
        return None


def _create_only_target_dag(target_dag_id: str) -> Optional[str]:
    """Create only the specific DAG needed for task execution.
    
    This is an optimization for Airflow 3.x where the SDK task runner re-imports
    the Python file for every task. Instead of recreating ALL dynamic DAGs
    (which could be 1000+), we parse the dag_id to determine its type and
    query the database directly by dag_id (which is now the primary key).
    
    Returns a status message or None if the DAG couldn't be created.
    """
    parsed = _parse_dag_id(target_dag_id)
    dag_type = parsed.get('type')
    
    if dag_type is None:
        _logger.debug(f"Unknown DAG type for {target_dag_id}, skipping optimization")
        return None
    
    dag_obj = None
    
    if dag_type == DAG_TYPE_INFRASTRUCTURE:
        # Infrastructure DAG is statically defined, no dynamic creation needed
        return f"Infrastructure DAG {target_dag_id} is static"
    
    elif dag_type == DAG_TYPE_INGESTION:
        # Parse platform to find the per-platform table, then lookup by dag_id
        platform = parsed.get('platform')
        if platform:
            dag_obj = _create_single_ingestion_dag(target_dag_id, platform)
            if dag_obj:
                globals()[target_dag_id] = dag_obj
                return f"Created single ingestion DAG: {target_dag_id}"
    
    elif dag_type == DAG_TYPE_DATATRANSFORMER:
        # Parse platform to find the per-platform table, then lookup by dag_id
        platform = parsed.get('platform')
        if platform:
            dag_obj = _create_single_datatransformer_dag(target_dag_id, platform)
            if dag_obj:
                globals()[target_dag_id] = dag_obj
                return f"Created single datatransformer DAG: {target_dag_id}"
    
    elif dag_type == DAG_TYPE_FACTORY:
        # Factory DAGs: lookup directly by dag_id
        dag_obj = _create_single_factory_dag(target_dag_id)
        if dag_obj:
            globals()[target_dag_id] = dag_obj
            return f"Created single factory DAG: {target_dag_id}"
    
    elif dag_type == DAG_TYPE_DT_FACTORY:
        # DT Factory DAGs: lookup directly by dag_id
        dag_obj = _create_single_dt_factory_dag(target_dag_id)
        if dag_obj:
            globals()[target_dag_id] = dag_obj
            return f"Created single DT factory DAG: {target_dag_id}"
    
    elif dag_type == DAG_TYPE_CQRS:
        # Direct lookup by dag_id - no need to parse prefix
        dag_obj = _create_single_cqrs_dag(target_dag_id)
        if dag_obj:
            globals()[target_dag_id] = dag_obj
            return f"Created single CQRS DAG: {target_dag_id}"
    
    # If we couldn't create the specific DAG, return None to fall back to full creation
    _logger.debug(f"Could not create single DAG for {target_dag_id}, type={dag_type}")
    return None

DATATRANSFORMER_DAG_DEFAULT_ARGS = {
    'owner': 'datasurface',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


def load_platform_configurations(config: PlatformConfig):
    """Load configurations from database and create DAG objects - identical to original template"""
    generated_dags = {}

    try:
        import sqlalchemy
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import URL

        # Create database connection using helper
        engine = build_merge_engine_from_env(config)
        if engine is None:
            expected_user, expected_pwd = get_merge_env_names(config)
            print(f"Missing Merge SQL credentials in environment variables: {expected_user}, {expected_pwd}")
            return {}

        try:
            # Read from the platform-specific airflow_dsg table
            # dag_id is now the primary key for direct lookup
            # Shard filter ensures each DAG file only loads its assigned DAGs
            table_name = config['phys_dag_table_name']
            rows = fetch_rows(engine, f"""
                    SELECT dag_id, config_json
                    FROM {table_name}
                    WHERE status = 'active'{_get_shard_filter()}
                """)
        finally:
            # Always dispose the engine to prevent connection pool leaks
            engine.dispose()

        # Map rows to DAGs using shared helper
        # dag_id is stored directly in the table, no need to reconstruct
        desired = map_rows_to_dags(
            rows,
            lambda r: (
                r[0],  # dag_id is the first column
                create_ingestion_stream_dag(config, json.loads(r[1]))
            )
        )
        generated_dags.update(desired)

    except Exception as e:
        print(f"Error loading platform configurations: {e}")
        return {}

    return generated_dags

def find_existing_factory_dags(platform_name: str):
    """Find all DAGs in globals() that were previously created by this ingestion factory"""
    factory_dags = {}
    platform_prefix = f"{platform_name}__"

    for name, obj in globals().items():
        if (name.startswith(platform_prefix) and
            name.endswith("_ingestion") and
            not name.endswith("_dt_ingestion") and  # Exclude DataTransformer output DAGs
            not name.endswith("_datatransformer") and  # Exclude DataTransformer execution DAGs
            hasattr(obj, 'dag_id')):
            factory_dags[name] = obj

    return factory_dags

def sync_dynamic_dags(config: PlatformConfig, **context):
    """Task that synchronizes dynamic DAGs with database configuration - identical to original template"""
    platform_name = config['platform_name']
    
    def log_info(msg):
        _logger.info(msg)
        print(msg)

    try:
        log_info("=" * 80)
        log_info("🏭 FACTORY DAG EXECUTION - Dynamic DAG Lifecycle Management")
        log_info(f"📍 Platform: {platform_name}")
        log_info("=" * 80)

        # Step 1: Find existing factory DAGs
        log_info("🔍 Scanning for existing factory-managed DAGs...")
        existing_dags = find_existing_factory_dags(platform_name)
        log_info(f"   Found {len(existing_dags)} existing DAGs from previous factory runs")

        # Step 2: Load current configurations from database
        log_info("📊 Loading current configurations from database...")
        current_dags = load_platform_configurations(config)
        log_info(f"   Found {len(current_dags)} active configurations in database")

        # Step 3: Calculate lifecycle changes (for summary) and apply
        existing_dag_ids = set(existing_dags.keys())
        current_dag_ids = set(current_dags.keys())
        to_create = current_dag_ids - existing_dag_ids
        to_remove = existing_dag_ids - current_dag_ids
        to_update = current_dag_ids & existing_dag_ids

        log_info("")
        log_info("📋 LIFECYCLE ANALYSIS:")
        log_info(f"   🆕 To Create: {len(to_create)} DAGs")
        log_info(f"   🗑️  To Remove: {len(to_remove)} DAGs")
        log_info(f"   🔄 To Update: {len(to_update)} DAGs")

        sync_dag_lifecycle(existing_dags, current_dags, log_info)

        total_changes = len(to_remove) + len(to_create) + len(to_update)
        log_info(f"📊 TOTAL CHANGES: {total_changes}")
        log_info(f"📊 ACTIVE DAGS: {len(current_dags)} (after lifecycle management)")

        summary = f"✅ Lifecycle complete: -{len(to_remove)} +{len(to_create)} ~{len(to_update)} = {len(current_dags)} active DAGs"
        return summary

    except Exception as e:
        _logger.error(f"❌ Factory DAG failed: {str(e)}")
        raise Exception(f"❌ Factory DAG failed: {str(e)}")

def create_platform_factory_dag(config: PlatformConfig) -> DAG:
    """Create a platform factory DAG from configuration - identical to yellow_platform_factory_dag.py.j2"""
    platform_name = config['platform_name']

    # Create the visible Factory DAG that appears in Airflow UI
    factory_dag = DAG(
        f'{platform_name}_factory_dag',
        description=f'Factory DAG for {platform_name} - Creates and manages dynamic ingestion stream DAGs',
        schedule='*/5 * * * *',  # Check for configuration changes every 5 minutes
        start_date=datetime(2025, 1, 1),
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=False,
        tags=['datasurface', 'ingestion_factory', 'factory', platform_name, 'dynamic-dag-manager']
    )

    # Create the factory task
    # Airflow 3.x: provide_context removed - context always provided
    sync_task = PythonOperator(
        task_id='sync_dynamic_dags',
        python_callable=lambda **context: sync_dynamic_dags(config, **context),
        dag=factory_dag
    )

    # Also execute once during DAG discovery for immediate availability
    # This provides backward compatibility with the current behavior
    try:
        initial_dags = load_platform_configurations(config)
        for dag_id, dag_object in initial_dags.items():
            globals()[dag_id] = dag_object
    except Exception as e:
        print(f"Warning: Failed to load initial dynamic DAGs during discovery: {e}")

    return factory_dag

# Copy the exact DataTransformer factory DAG creation logic from the original template
# This is identical to datatransformer_factory_dag.py.j2 functionality

def determine_next_action(**context):
    """Determine whether to reschedule or wait using shared log parsing (strict mode for DataTransformer)."""
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    task_id_suffix = context.get('task_id_suffix', 'job')
    task_id = f'datatransformer_{task_id_suffix}' if 'datatransformer' in dag_id else f'output_ingestion_{task_id_suffix}'
    content = read_latest_task_log(dag_id, run_id, task_id)
    result = choose_next_action_from_log(content, strict=True)
    
    # Airflow 3.x: Explicitly push XCom to ensure downstream tasks can detect success
    # BranchPythonOperator auto-push may not work reliably in Airflow 3.x SDK
    ti = context['ti']
    ti.xcom_push(key='branch_success', value=True)
    
    return result

def create_datatransformer_execution_dag(platform_config: PlatformConfig, dt_config: DataTransformerConfig) -> DAG:
    """Create a DataTransformer execution DAG from configuration - identical to original template"""
    platform_name = platform_config['platform_name']
    workspace_name = dt_config['workspace_name']
    output_datastore_name = dt_config['output_datastore_name']
    input_dag_ids = dt_config.get('input_dag_ids', [])
    schedule_string = dt_config.get('schedule_string')  # None if sensor-based, cron string if scheduled
    tag_list = dt_config['tags']
    # Default arguments for the DAG
    default_args = DATATRANSFORMER_DAG_DEFAULT_ARGS

    # Determine schedule: use cron string if provided, otherwise None (external trigger/sensor based)
    dag_schedule = schedule_string if schedule_string else None

    # Create the DAG
    dag = DAG(
        f'{platform_name}__{workspace_name}_datatransformer',
        default_args=default_args,
        description=f'DataTransformer DAG for {platform_name}__{workspace_name}',
        schedule=dag_schedule,  # Use trigger schedule or None for sensor-based
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=False,  # Start unpaused so DAGs are immediately active
        tags=tag_list
    )

    # Build base env vars shared across tasks (platform + git token + OTLP)
    base_env_vars = [
        k8s.V1EnvVar(name='DATASURFACE_PLATFORM_NAME', value=platform_config['original_platform_name']),
        k8s.V1EnvVar(name='DATASURFACE_NAMESPACE', value=platform_config['namespace_name']),
    ]

    # Always include the platform git credential env vars using typed helper
    base_env_vars.extend(build_env_vars_for_credential(
        platform_config['git_credential_secret_name'],
        platform_config['git_credential_secret_type']
    ))

    # OpenTelemetry OTLP configuration for node-local agent access
    base_env_vars.extend(build_otlp_env_vars(platform_config))

    # Merge credential env vars (for infra/reset/output tasks)
    merge_env_vars = build_env_vars_for_credential(
        platform_config['merge_db_credential_secret_name'],
        platform_config['merge_db_credential_secret_type']
    )

    # DataTransformer credential env vars (required for transformer task)
    dt_only_env_vars = []
    if not dt_config.get('dt_credential_secret_name'):
        raise ValueError(f"DataTransformer configuration for workspace {dt_config['workspace_name']} is missing required dt_credential_secret_name")
    
    dt_env_base = dt_config['dt_credential_secret_name']
    dt_type = dt_config.get('dt_credential_secret_type')
    dt_only_env_vars.extend(build_env_vars_for_credential(dt_env_base, dt_type))

    # If DataTransformer uses a different git credential, add that TOKEN only to the transformer env
    transformer_extra_git_env = []
    if (dt_config.get('git_credential_secret_name') and
        dt_config['git_credential_secret_name'] != platform_config['git_credential_secret_name']):
        transformer_extra_git_env.extend(build_env_vars_for_credential(
            dt_config['git_credential_secret_name'],
            dt_config.get('git_credential_secret_type', 'API_TOKEN')
        ))

    # Optional: extra credentials for DataTransformer pods (typed entries: [name, type])
    dt_extra_env_vars = []
    extra_creds = dt_config.get('dt_extra_credentials', [])
    if extra_creds:
        for cred_entry in extra_creds:
            if not isinstance(cred_entry, (list, tuple)) or len(cred_entry) != 2:
                continue
            cred_name, cred_type = cred_entry[0], cred_entry[1]
            dt_extra_env_vars.extend(build_env_vars_for_credential(cred_name, cred_type))

    # CRG credential env vars (for output ingestion to read from DataTransformer output database)
    crg_env_vars = []
    if dt_config.get('dt_crg_credential_secret_name'):
        crg_user, crg_password = secret_manager.getUserPassword(dt_config['dt_crg_credential_secret_name'])
        crg_env_vars.extend([
            SecretManager.to_envvar(f"{dt_config['dt_crg_credential_secret_name']}_USER", crg_user),
            SecretManager.to_envvar(f"{dt_config['dt_crg_credential_secret_name']}_PASSWORD", crg_password)
        ])

    # Event publish credential env vars (for Kafka event publishing)
    event_publish_env_vars = []
    if platform_config.get('event_publish_credential_secret_name'):
        event_publish_env_vars.extend(build_env_vars_for_credential(
            platform_config['event_publish_credential_secret_name'],
            platform_config.get('event_publish_credential_secret_type', 'USER_PASSWORD')
        ))

    def dedupe_env_vars(*env_var_lists):
        """Combine and deduplicate environment variables by name"""
        env_dict = {}
        for env_list in env_var_lists:
            for env_var in env_list:
                env_dict[env_var.name] = env_var
        return list(env_dict.values())

    # Final env var sets - deduplicated
    infra_env_vars = dedupe_env_vars(base_env_vars, merge_env_vars, event_publish_env_vars)
    datatransformer_env_vars = dedupe_env_vars(base_env_vars, merge_env_vars, transformer_extra_git_env, dt_only_env_vars, dt_extra_env_vars, event_publish_env_vars)
    output_ingestion_env_vars = dedupe_env_vars(base_env_vars, merge_env_vars, transformer_extra_git_env, dt_only_env_vars, crg_env_vars, event_publish_env_vars)

    # Start task
    start_task = EmptyOperator(
        task_id='start_datatransformer',
        dag=dag
    )

    # Create external task sensors only if no schedule (sensor-based mode)
    sensors = []
    if schedule_string is None:
        # Sensor-based mode: wait for input DAGs to complete
        for i, input_dag_id in enumerate(input_dag_ids, 1):
            sensor = ExternalTaskSensor(
                task_id=f'wait_for_{input_dag_id.replace("__", "_").replace("-", "_")}',
                external_dag_id=input_dag_id,
                external_task_id='wait_for_trigger',  # Wait for the completion of the ingestion
                allowed_states=['success'],
                failed_states=['failed'],
                mode='reschedule',
                timeout=3600,  # 1 hour timeout
                poke_interval=30,  # Check every 30 seconds
                dag=dag
            )
            sensors.append(sensor)
    # If scheduled mode: no sensors needed, DAG runs on schedule

    # Git cache mounts
    _vols, _mounts = git_cache_mounts(platform_config.get('git_cache_enabled'))

    # DataTransformer job - handles its own batch reset internally
    # Use dbt-specific docker image if code_artifact_type is 'dbt', otherwise use default datasurface image
    dt_docker_image = dt_config.get('docker_image', platform_config['datasurface_docker_image'])
    datatransformer_job = build_pod_operator(
        task_id='datatransformer_job',
        name=f"{platform_name}-{workspace_name.replace('_', '-')}-dt-job",
        namespace=platform_config['namespace_name'],
        image=dt_docker_image,
        cmds=['python', '-m', 'datasurface.platforms.yellow.transformerjob'],
        arguments=[
            '--platform-name', platform_config['original_platform_name'],
            '--workspace-name', workspace_name,
            '--operation', 'run-datatransformer',
            '--working-folder', '/workspace/datatransformer',
            '--git-repo-path', '/cache/git-cache',  # Use cache mount path
            '--git-repo-type', platform_config['git_repo_type'],
            '--git-repo-url', platform_config['git_repo_url'],
            '--git-repo-owner', platform_config['git_repo_owner'],
            '--git-repo-name', platform_config['git_repo_repo_name'],
            '--git-repo-branch', platform_config['git_repo_branch'],
            '--git-platform-repo-credential-name', platform_config['git_credential_name'],
            '--rte-name', 'demo',
            '--max-cache-age-minutes', str(platform_config['git_cache_max_age_minutes'])  # Cache freshness threshold
        ] + (['--git-release-selector-hex', platform_config['git_release_selector']] if platform_config.get('git_release_selector') else []) \
          + (['--use-git-cache'] if platform_config.get('git_cache_enabled') else []),
        env_vars=datatransformer_env_vars,
        image_pull_policy='IfNotPresent',
        volumes=_vols + [
            k8s.V1Volume(
                name='datatransformer-workspace',
                empty_dir=k8s.V1EmptyDirVolumeSource()
            )
        ],
        volume_mounts=_mounts + [
            k8s.V1VolumeMount(
                name='datatransformer-workspace',
                mount_path='/workspace/datatransformer',
                read_only=False
            )
        ],
        resources=pod_resources_from_limits(
            dt_config.get('job_limits', {}),
            {'requested_memory': '512Mi', 'requested_cpu': '200m', 'limits_memory': '2Gi', 'limits_cpu': '1000m'}
        ),
        dag=dag,
        priority_weight=dt_config.get('priority', 0),
        weight_rule='absolute',
        do_xcom_push=False,
    )

    # Output ingestion job as a task within the transformer DAG - only runs if datatransformer succeeds
    output_ingestion_job = build_pod_operator(
        task_id='output_ingestion_job',
        name=f"{platform_name}-{output_datastore_name.replace('_', '-')}-output-job",
        namespace=platform_config['namespace_name'],
        image=platform_config['datasurface_docker_image'],
        cmds=['python', '-m', 'datasurface.platforms.yellow.jobs'],
        arguments=[
            '--platform-name', platform_config['original_platform_name'],
            '--operation', 'snapshot-merge',
            '--store-name', output_datastore_name,
            '--git-repo-path', '/cache/git-cache',  # Use cache mount path
            '--git-repo-type', platform_config['git_repo_type'],
            '--git-repo-url', platform_config['git_repo_url'],
            '--git-repo-owner', platform_config['git_repo_owner'],
            '--git-repo-name', platform_config['git_repo_repo_name'],
            '--git-repo-branch', platform_config['git_repo_branch'],
            '--git-platform-repo-credential-name', platform_config['git_credential_name'],
            '--rte-name', 'demo',
            '--max-cache-age-minutes', str(platform_config['git_cache_max_age_minutes'])  # Cache freshness threshold
        ] + (['--git-release-selector-hex', platform_config['git_release_selector']] if platform_config.get('git_release_selector') else []) \
          + (['--use-git-cache'] if platform_config.get('git_cache_enabled') else []),
        env_vars=output_ingestion_env_vars,
        image_pull_policy='IfNotPresent',
        volumes=_vols,
        volume_mounts=_mounts,
        do_xcom_push=False,
        trigger_rule='all_success',  # Only run if datatransformer_job succeeds
        resources=pod_resources_from_limits(
            dt_config.get('output_job_limits', {}),
            {'requested_memory': '256Mi', 'requested_cpu': '100m', 'limits_memory': '1Gi', 'limits_cpu': '500m'}
        ),
        dag=dag,
        priority_weight=dt_config.get('priority', 0),
        weight_rule='absolute'
    )

    # Branch operator to determine next action (after output ingestion completes)
    branch = BranchPythonOperator(
        task_id='check_result',
        python_callable=determine_next_action,
        op_kwargs={'task_id_suffix': 'job'},
        dag=dag,
        trigger_rule='all_success'  # Only run if all upstream tasks succeed - failures should fail the DAG
    )

    # Cleanup task - check for failures and propagate to fail the DAG run
    # Airflow 3.x Bug Workaround: DAG runs show as success if final leaf task succeeds,
    # even when upstream tasks failed. We must explicitly check and raise an exception.
    def cleanup_and_check_failures(**context):
        # Airflow 3.x: Check if branch task (check_result) ran successfully
        # We use an explicit XCom key 'branch_success' pushed by determine_next_action
        ti = context['ti']
        
        # Check for our explicit success marker pushed by the branch task
        branch_success = ti.xcom_pull(task_ids='check_result', key='branch_success')
        
        if branch_success is True:
            # Branch ran successfully - this means datatransformer_job and output_ingestion_job succeeded
            return "Cleanup complete - DAG succeeded"
        
        # Branch didn't run or didn't push success marker - critical tasks must have failed
        raise Exception(
            "DataTransformer DAG failed - critical tasks (datatransformer_job or output_ingestion_job) "
            "did not complete successfully. Check task logs for details."
        )

    # Airflow 3.x: provide_context removed - context always provided
    # This task runs after all others complete and checks for failures
    cleanup_batch_id = PythonOperator(
        task_id='cleanup_dt_batch_id',
        python_callable=cleanup_and_check_failures,
        dag=dag,
        trigger_rule='all_done'
    )

    # Reschedule immediately for continuous processing
    reschedule = TriggerDagRunOperator(
        task_id='reschedule_immediately',
        trigger_dag_id=f'{platform_name}__{workspace_name}_datatransformer',
        conf={'triggered_by': 'reschedule'},
        dag=dag
    )

    # Wait for next external trigger
    wait = EmptyOperator(
        task_id='wait_for_trigger',
        dag=dag
    )

    # Define task dependencies
    if sensors:
        start_task >> sensors
        sensors >> datatransformer_job
    else:
        start_task >> datatransformer_job

    # Sequential execution with proper failure handling:
    # - output_ingestion_job only runs if datatransformer_job succeeds (trigger_rule='all_success')
    # - branch runs after both complete (either success path or failure path)
    datatransformer_job >> output_ingestion_job
    [datatransformer_job, output_ingestion_job] >> branch
    branch >> [reschedule, wait]
    [reschedule, wait] >> cleanup_batch_id

    return dag

def load_datatransformer_configurations(config: PlatformConfig):
    """Load DataTransformer configurations from database and create DAG objects - identical to original template"""
    generated_dags = {}

    try:
        import sqlalchemy
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import URL

        # Create database connection using helper
        engine = build_merge_engine_from_env(config)
        if engine is None:
            expected_user, expected_pwd = get_merge_env_names(config)
            print(f"Missing Merge SQL credentials in environment variables: {expected_user}, {expected_pwd}")
            return {}

        try:
            # Read from the platform-specific airflow_datatransformer table
            # dag_id is now the primary key for direct lookup
            # Shard filter ensures each DAG file only loads its assigned DAGs
            table_name = config['phys_datatransformer_table_name']
            rows = fetch_rows(engine, f"""
                    SELECT dag_id, config_json
                    FROM {table_name}
                    WHERE status = 'active'{_get_shard_filter()}
                """)
        finally:
            # Always dispose the engine to prevent connection pool leaks
            engine.dispose()

        def _log_and_build_dt_dag(platform_config: dict, dt_cfg: dict):
            # Log scheduling mode for debugging (only during discovery)
            # workspace_name is stored in config_json for job parameters
            workspace_name = dt_cfg.get('workspace_name', 'unknown')
            if not _silent_mode:
                schedule_string = dt_cfg.get('schedule_string')
                if schedule_string:
                    print(f"Creating scheduled DataTransformer DAG for {workspace_name} with schedule: {schedule_string}")
                else:
                    print(f"Creating sensor-based DataTransformer DAG for {workspace_name}")
            return create_datatransformer_execution_dag(platform_config, dt_cfg)

        # Map rows to DAGs using shared helper
        # dag_id is stored directly in the table, no need to reconstruct
        desired = map_rows_to_dags(
            rows,
            lambda r: (
                r[0],  # dag_id is the first column
                _log_and_build_dt_dag(config, json.loads(r[1]))
            )
        )

        generated_dags.update(desired)
        return generated_dags

    except Exception as e:
        print(f"Error loading DataTransformer configurations: {e}")
        return {}

def sync_datatransformer_dags(config: PlatformConfig, **context):
    """Synchronize dynamic DataTransformer DAGs with database configuration - identical to original template"""
    platform_name = config['platform_name']
    
    def log_info(msg):
        _logger.info(msg)
        print(msg)

    try:
        log_info("=" * 80)
        log_info("🏭 DATATRANSFORMER FACTORY DAG EXECUTION STARTED")
        log_info("=" * 80)
        log_info(f"🔧 Platform: {platform_name}")
        log_info(f"🗃️  DataTransformer Table: {config['phys_datatransformer_table_name']}")
        log_info("=" * 80)

        # Step 1: Load current configuration from database
        log_info("📊 LOADING DATATRANSFORMER CONFIGURATIONS FROM DATABASE")
        log_info("=" * 60)

        current_dags = load_datatransformer_configurations(config)
        log_info(f"📈 Found {len(current_dags)} DAG configurations in database")

        # Step 2: Compare with existing DAGs in globals
        # Get existing DataTransformer-related DAGs using naming convention
        # Note: _dt_ingestion DAGs are now integrated as tasks, so only check _datatransformer DAGs
        existing_dt_dags = {k: v for k, v in globals().items()
                           if k.endswith('_datatransformer')}

        current_dag_ids = set(current_dags.keys())
        existing_dag_ids = set(existing_dt_dags.keys())

        to_create = current_dag_ids - existing_dag_ids
        to_remove = existing_dag_ids - current_dag_ids
        to_update = current_dag_ids & existing_dag_ids

        log_info(f"🆕 DAGs to CREATE: {len(to_create)}")
        log_info(f"🗑️  DAGs to REMOVE: {len(to_remove)}")
        log_info(f"🔄 DAGs to UPDATE: {len(to_update)}")

        # Step 3: Execute lifecycle changes via shared helper
        sync_dag_lifecycle(existing_dt_dags, current_dags, log_info)

        total_changes = len(to_remove) + len(to_create) + len(to_update)
        log_info(f"📊 TOTAL CHANGES: {total_changes}")
        log_info(f"📊 ACTIVE DATATRANSFORMER DAGS: {len(current_dags)}")

        summary = f"✅ DataTransformer Lifecycle complete: -{len(to_remove)} +{len(to_create)} ~{len(to_update)} = {len(current_dags)} active DAGs"
        return summary

    except Exception as e:
        _logger.error(f"❌ DATATRANSFORMER FACTORY DAG EXECUTION FAILED: {str(e)}")
        raise Exception(f"❌ DataTransformer Factory DAG failed: {str(e)}")

def create_datatransformer_factory_dag(config: PlatformConfig) -> DAG:
    """Create a datatransformer factory DAG from configuration - identical to datatransformer_factory_dag.py.j2"""
    platform_name = config['platform_name']

    # Create the factory DAG for DataTransformers
    factory_default_args = {
        'owner': 'datasurface',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    factory_dag = DAG(
        f'{platform_name}_datatransformer_factory',
        default_args=factory_default_args,
        description=f'DataTransformer Dynamic DAG Factory for {platform_name}',
        schedule='*/5 * * * *',  # Run every 5 minutes to sync configurations
        catchup=False,
        max_active_runs=1,
        tags=['datasurface', 'datatransformer_factory', 'factory', platform_name],
        is_paused_upon_creation=False  # Start unpaused so DAG is immediately active
    )

    # Create the factory task
    # Airflow 3.x: provide_context removed - context always provided
    sync_task = PythonOperator(
        task_id='sync_datatransformer_dags',
        python_callable=lambda **context: sync_datatransformer_dags(config, **context),
        dag=factory_dag
    )

    # Also execute once during DAG discovery for immediate availability
    try:
        initial_dags = load_datatransformer_configurations(config)
        for dag_id, dag_object in initial_dags.items():
            globals()[dag_id] = dag_object
    except Exception as e:
        print(f"Warning: Failed to load initial DataTransformer DAGs during discovery: {e}")

    return factory_dag

def create_dc_reconcile_dag(config: dict) -> DAG:
    """Create a DC reconcile DAG from configuration - reconciles all workspaces on a specific DataContainer"""
    psp_name = config['psp_name']
    data_container_name = config['data_container_name']
    schedule_string = config['schedule_string']
    sources = config.get('sources', [])
    tag_list = config.get('tags', ['datasurface', 'reconcile', 'dc_reconcile'])

    # Create unique DAG ID
    dag_id = f"{psp_name}_{data_container_name}_reconcile"

    # Default arguments for DC reconcile DAGs
    default_args = {
        'owner': 'datasurface',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,  # Allow one retry for reconcile
        'retry_delay': timedelta(minutes=10),
    }

    # Create the DAG
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'DC reconcile DAG for {psp_name}/{data_container_name} (sources: {", ".join(sources)})',
        schedule=schedule_string,
        catchup=False,
        max_active_runs=1,
        tags=tag_list,
        is_paused_upon_creation=False
    )

    # Environment variables for the reconcile job
    merge_user, merge_password = secret_manager.getUserPassword(config['merge_db_credential_secret_name'])
    pat_secret = secret_manager.getPATSecret(config['git_credential_secret_name'])
    env_vars = [
        k8s.V1EnvVar(name='DATASURFACE_PSP_NAME', value=psp_name),
        k8s.V1EnvVar(name='DATASURFACE_NAMESPACE', value=config['namespace_name']),
        SecretManager.to_envvar(f"{config['merge_db_credential_secret_name']}_USER", merge_user),
        SecretManager.to_envvar(f"{config['merge_db_credential_secret_name']}_PASSWORD", merge_password),
        SecretManager.to_envvar(f"{config['git_credential_secret_name']}_TOKEN", pat_secret),
    ]
    # OpenTelemetry OTLP configuration
    env_vars.extend(build_otlp_env_vars(config))

    # Add DataContainer credential (needed to reconcile views on that container)
    if config.get('dc_credential_secret_name'):
        dc_user, dc_password = secret_manager.getUserPassword(config['dc_credential_secret_name'])
        env_vars.extend([
            SecretManager.to_envvar(f"{config['dc_credential_secret_name']}_USER", dc_user),
            SecretManager.to_envvar(f"{config['dc_credential_secret_name']}_PASSWORD", dc_password),
        ])

    # Event publish credential (for Kafka event publishing)
    if config.get('event_publish_credential_secret_name'):
        env_vars.extend(build_env_vars_for_credential(
            config['event_publish_credential_secret_name'],
            config.get('event_publish_credential_secret_type', 'USER_PASSWORD')
        ))

    # Git cache mounts
    _vols, _mounts = git_cache_mounts(config.get('git_cache_enabled'))

    # DC reconcile job
    reconcile_job = build_pod_operator(
        task_id='dc_reconcile_job',
        name=f"{psp_name}-{data_container_name}-reconcile",
        namespace=config['namespace_name'],
        image=config['datasurface_docker_image'],
        cmds=['python', '-m', 'datasurface.platforms.yellow.reconcile_workspace_views'],
        arguments=[
            '--psp', psp_name,
            '--data-container', data_container_name,
            '--git-repo-path', '/cache/git-cache',
            '--git-repo-type', config['git_repo_type'],
            '--git-repo-url', config['git_repo_url'],
            '--git-repo-owner', config['git_repo_owner'],
            '--git-repo-name', config['git_repo_repo_name'],
            '--git-repo-branch', config['git_repo_branch'],
            '--git-platform-repo-credential-name', config['git_credential_name'],
            '--rte-name', 'demo',
            '--max-cache-age-minutes', str(config.get('git_cache_max_age_minutes', 5))
        ] + (['--git-release-selector-hex', config['git_release_selector']] if config.get('git_release_selector') else []) \
          + (['--use-git-cache'] if config.get('git_cache_enabled') else []),
        env_vars=env_vars,
        image_pull_policy='IfNotPresent',
        volumes=_vols,
        volume_mounts=_mounts,
        resources=pod_resources_from_limits(
            config.get('job_limits', {}),
            {'requested_memory': '512Mi', 'requested_cpu': '200m', 'limits_memory': '2Gi', 'limits_cpu': '1000m'}
        ),
        dag=dag,
        priority_weight=0,
        weight_rule='absolute',
        do_xcom_push=False,
    )

    return dag

def create_cqrs_execution_dag(config: dict) -> DAG:
    """Create a CQRS execution DAG from configuration - runs CQRS sync job for a specific PSP/CRG/DataContainer"""
    psp_name = config['psp_name']
    crg_name = config['crg_name']
    data_container_name = config['data_container_name']
    schedule_string = config['schedule_string']
    
    # Create unique DAG ID
    dag_id = f"{psp_name}_{crg_name}_{data_container_name}_cqrs"
    
    # Default arguments for CQRS DAGs
    default_args = {
        'owner': 'datasurface',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,  # Allow one retry for CQRS sync
        'retry_delay': timedelta(minutes=10),
    }
    
    # Create the DAG
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'CQRS sync DAG for {psp_name}/{crg_name}/{data_container_name}',
        schedule=schedule_string,  # Run every X minutes - configurable per deployment needs
        catchup=False,
        max_active_runs=1,
        tags=['datasurface', 'cqrs_sync', psp_name, crg_name, f'{psp_name}.{crg_name}', data_container_name],
        is_paused_upon_creation=False
    )
    
    # Environment variables for the CQRS job - build manually for CQRS
    merge_user, merge_password = secret_manager.getUserPassword(config['merge_db_credential_secret_name'])
    pat_secret = secret_manager.getPATSecret(config['git_credential_secret_name'])
    cqrs_user, cqrs_password = secret_manager.getUserPassword(config['cqrs_credential_secret_name'])
    env_vars = [
        k8s.V1EnvVar(name='DATASURFACE_PSP_NAME', value=psp_name),
        k8s.V1EnvVar(name='DATASURFACE_NAMESPACE', value=config['namespace_name']),
        SecretManager.to_envvar(f"{config['merge_db_credential_secret_name']}_USER", merge_user),
        SecretManager.to_envvar(f"{config['merge_db_credential_secret_name']}_PASSWORD", merge_password),
        SecretManager.to_envvar(f"{config['git_credential_secret_name']}_TOKEN", pat_secret),
        SecretManager.to_envvar(f"{config['cqrs_credential_secret_name']}_USER", cqrs_user),
        SecretManager.to_envvar(f"{config['cqrs_credential_secret_name']}_PASSWORD", cqrs_password),
    ]
    # OpenTelemetry OTLP configuration
    env_vars.extend(build_otlp_env_vars(config))

    # Event publish credential (for Kafka event publishing)
    if config.get('event_publish_credential_secret_name'):
        env_vars.extend(build_env_vars_for_credential(
            config['event_publish_credential_secret_name'],
            config.get('event_publish_credential_secret_type', 'USER_PASSWORD')
        ))
    
    # Git cache mounts
    _vols, _mounts = git_cache_mounts(config.get('git_cache_enabled'))
    
    # CQRS sync job
    cqrs_job = build_pod_operator(
        task_id='cqrs_sync_job',
        name=f"{psp_name}-{crg_name}-{data_container_name}-cqrs",
        namespace=config['namespace_name'],
        image=config['datasurface_docker_image'],
        cmds=['python', '-m', 'datasurface.platforms.yellow.jobs_cqrs'],
        arguments=[
            '--psp-name', psp_name,
            '--crg-name', crg_name,
            '--data-container-name', data_container_name,
            '--operation', 'cqrs-sync',
            '--git-repo-path', '/cache/git-cache',  # Use cache mount path
            '--git-repo-type', config['git_repo_type'],
            '--git-repo-url', config['git_repo_url'],
            '--git-repo-owner', config['git_repo_owner'],
            '--git-repo-name', config['git_repo_repo_name'],
            '--git-repo-branch', config['git_repo_branch'],
            '--git-platform-repo-credential-name', config['git_credential_name'],
            '--rte-name', 'demo',
            '--max-cache-age-minutes', str(config.get('git_cache_max_age_minutes', 5))
        ] + (['--git-release-selector', config['git_release_selector']] if config.get('git_release_selector') else []) \
          + (['--use-git-cache'] if config.get('git_cache_enabled') else []),
        env_vars=env_vars,
        image_pull_policy='IfNotPresent',
        volumes=_vols,
        volume_mounts=_mounts,
        resources=pod_resources_from_limits(
            config.get('job_limits', {}),
            {'requested_memory': '512Mi', 'requested_cpu': '200m', 'limits_memory': '1Gi', 'limits_cpu': '500m'}
        ),
        dag=dag,
        priority_weight=0,
        weight_rule='absolute',
        do_xcom_push=False,
    )
    
    return dag

def load_cqrs_configurations() -> dict:
    """Load CQRS configurations from database and create CQRS DAG objects"""
    generated_dags = {}
    
    try:
        import sqlalchemy
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import URL
        
        # Create database connection using helper
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host='postgres-co',
            port=5432,
            database='merge_db',
            query_driver=''
        )
        if engine is None:
            expected_user, expected_pwd = get_merge_env_names_template('postgres-demo-merge')
            print(f"Missing Merge SQL credentials in environment variables: {expected_user}, {expected_pwd}")
            return {}
        
        try:
            # Read from the PSP-specific CQRS table using template parameter
            # dag_id is now the primary key for direct lookup
            # Shard filter ensures each DAG file only loads its assigned DAGs
            cqrs_table_name = 'demo_psp_cqrs_dags'
            rows = fetch_rows(engine, f"""
                SELECT dag_id, config_json
                FROM {cqrs_table_name}
                WHERE status = 'active'{_get_shard_filter()}
            """)
        finally:
            # Always dispose the engine to prevent connection pool leaks
            engine.dispose()

        # Map rows to DAGs using shared helper
        # dag_id is stored directly in the table, no need to reconstruct
        desired = map_rows_to_dags(
            rows,
            lambda r: (
                r[0],  # dag_id is the first column
                create_cqrs_execution_dag(json.loads(r[1]))
            )
        )
        generated_dags.update(desired)
        
    except Exception as e:
        print(f"Error loading CQRS configurations: {e}")
        return {}
    
    return generated_dags

def load_dc_reconcile_configurations() -> dict:
    """Load DC reconcile configurations from database and create DC reconcile DAG objects"""
    generated_dags = {}

    try:
        import sqlalchemy
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import URL

        # Create database connection using helper
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host='postgres-co',
            port=5432,
            database='merge_db',
            query_driver=''
        )
        if engine is None:
            expected_user, expected_pwd = get_merge_env_names_template('postgres-demo-merge')
            print(f"Missing Merge SQL credentials in environment variables: {expected_user}, {expected_pwd}")
            return {}

        try:
            # Read from the PSP-specific DC reconcile table
            # dag_id is now the primary key for direct lookup
            # Shard filter ensures each DAG file only loads its assigned DAGs
            dc_reconcile_table_name = 'demo_psp_dc_reconcile_dags'
            rows = fetch_rows(engine, f"""
                SELECT dag_id, config_json
                FROM {dc_reconcile_table_name}
                WHERE status = 'active'{_get_shard_filter()}
            """)
        finally:
            # Always dispose the engine to prevent connection pool leaks
            engine.dispose()

        # Map rows to DAGs using shared helper
        # dag_id is stored directly in the table, no need to reconstruct
        desired = map_rows_to_dags(
            rows,
            lambda r: (
                r[0],  # dag_id is the first column
                create_dc_reconcile_dag(json.loads(r[1]))
            )
        )
        generated_dags.update(desired)

    except Exception as e:
        print(f"Error loading DC reconcile configurations: {e}")
        return {}

    return generated_dags

def sync_cqrs_dags(**context):
    """Synchronize dynamic CQRS DAGs with database configuration"""
    # Check if we're running during task execution (context provided) or module import (no context)
    is_task_execution = context.get('task_instance') is not None if context else False
    
    def log_message(msg):
        # Airflow 3.x: Use standard Python logging
        _logger.info(msg)
        print(msg)  # Also print for stdout capture
    
    try:
        log_message("=" * 80)
        log_message("🔄 CQRS DAG EXECUTION STARTED")
        log_message("=" * 80)
        
        # Step 1: Load current configuration from database
        log_message("📊 LOADING CQRS CONFIGURATIONS FROM DATABASE")
        log_message("=" * 60)
        
        current_dags = load_cqrs_configurations()
        log_message(f"📈 Found {len(current_dags)} CQRS DAG configurations in database")
        
        # Step 2: Compare with existing DAGs in globals
        existing_cqrs_dags = {k: v for k, v in globals().items()
                              if k.endswith('_cqrs') and hasattr(v, 'dag_id')}
        
        current_dag_ids = set(current_dags.keys())
        existing_dag_ids = set(existing_cqrs_dags.keys())
        
        to_create = current_dag_ids - existing_dag_ids
        to_remove = existing_dag_ids - current_dag_ids
        to_update = current_dag_ids & existing_dag_ids
        
        log_message(f"🆕 CQRS DAGs to CREATE: {len(to_create)}")
        log_message(f"🗑️  CQRS DAGs to REMOVE: {len(to_remove)}")
        log_message(f"🔄 CQRS DAGs to UPDATE: {len(to_update)}")
        
        # Step 3: Execute lifecycle changes via shared helper
        sync_dag_lifecycle(existing_cqrs_dags, current_dags, log_message)
        
        total_changes = len(to_remove) + len(to_create) + len(to_update)
        log_message(f"📊 TOTAL CQRS CHANGES: {total_changes}")
        log_message(f"📊 ACTIVE CQRS DAGS: {len(current_dags)}")
        
        summary = f"✅ CQRS Lifecycle complete: -{len(to_remove)} +{len(to_create)} ~{len(to_update)} = {len(current_dags)} active DAGs"
        return summary
        
    except Exception as e:
        error_msg = f"❌ CQRS DAG EXECUTION FAILED: {str(e)}"
        log_message(error_msg)
        if is_task_execution:
            raise Exception(f"❌ CQRS DAG failed: {str(e)}")
        else:
            return f"CQRS DAG creation failed: {str(e)}"

def create_factory_dags_from_database(silent: bool = False, **context):
    """Read factory DAG configurations from database and create factory DAGs in globals()
    
    Args:
        silent: If True, suppress verbose logging (used during task execution to avoid noise)
    """
    # Check if we're running during task execution (context provided) or module import (no context)
    is_task_execution = context.get('task_instance') is not None if context else False

    def log_message(msg):
        if silent:
            return  # Suppress logging during task execution mode
        # Airflow 3.x: Use standard Python logging
        _logger.info(msg)
        print(msg)  # Also print for stdout capture

    try:
        import sqlalchemy
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import URL

        # Build database connection using secret_manager (reads from Kubernetes API)
        try:
            merge_db_user, merge_db_password = secret_manager.getUserPassword('postgres-demo-merge')
        except Exception as e:
            if is_task_execution:
                raise Exception(f"Failed to get Merge SQL credentials: {e}")
            else:
                log_message(f"Missing Merge SQL credentials - factory DAGs will be created during task execution: {e}")
                return "Factory DAG creation deferred to task execution"

        merge_db_host = 'postgres-co'
        merge_db_port = 5432
        merge_db_db_name = 'merge_db'
        merge_db_query = ''

        # Create database connection using helper function
        engine = build_merge_engine_from_env_template(
            credential_secret_name='postgres-demo-merge',
            credential_secret_type='USER_PASSWORD',
            driver='postgresql',
            host=merge_db_host,
            port=merge_db_port,
            database=merge_db_db_name,
            query_driver=merge_db_query
        )
        if engine is None:
            expected_user, expected_pwd = get_merge_env_names_template('postgres-demo-merge')
            if is_task_execution:
                raise Exception(f"Missing Merge SQL credentials in environment variables: {expected_user}, {expected_pwd}")
            else:
                log_message(f"Missing Merge SQL credentials ({expected_user}, {expected_pwd}) - factory DAGs will be created during task execution")
                return "Factory DAG creation deferred to task execution"

        try:
            # Read factory DAG configurations - table created/populated by handleModelMerge
            # dag_id is now the primary key for direct lookup
            # Shard filter ensures each DAG file only loads its assigned factory DAGs
            factory_table_name = 'demo_psp_factory_dags'
            with engine.begin() as connection:
                result = connection.execute(text(f"""
                    SELECT dag_id, config_json
                    FROM {factory_table_name}
                    WHERE status = 'active'{_get_shard_filter()}
                """))
                configs = result.fetchall()
        finally:
            # Always dispose the engine to prevent connection pool leaks
            engine.dispose()

        # Build desired factory DAGs from configurations (per-row)
        # dag_id format: {platform}_factory_dag or {platform}_datatransformer_factory
        desired_factories = {}
        for dag_id, config_json in configs:
            config = json.loads(config_json)
            if dag_id.endswith('_factory_dag'):
                desired_factories[dag_id] = create_platform_factory_dag(config)
            elif dag_id.endswith('_datatransformer_factory'):
                desired_factories[dag_id] = create_datatransformer_factory_dag(config)

        # Also load CQRS DAGs from their table
        log_message("📊 LOADING CQRS CONFIGURATIONS")
        cqrs_dags = load_cqrs_configurations()
        desired_factories.update(cqrs_dags)
        log_message(f"📈 Added {len(cqrs_dags)} CQRS DAG configurations")

        # Also load DC reconcile DAGs from their table
        log_message("📊 LOADING DC RECONCILE CONFIGURATIONS")
        dc_reconcile_dags = load_dc_reconcile_configurations()
        desired_factories.update(dc_reconcile_dags)
        log_message(f"📈 Added {len(dc_reconcile_dags)} DC reconcile DAG configurations")

        # Find existing factory, CQRS, and DC reconcile DAGs created by this module
        existing_factories = {k: v for k, v in globals().items()
                              if (k.endswith('_factory_dag') or k.endswith('_datatransformer_factory') or
                                  k.endswith('_cqrs') or k.endswith('_reconcile')) and hasattr(v, 'dag_id')}

        # Analyze lifecycle for summary
        existing_ids = set(existing_factories.keys())
        desired_ids = set(desired_factories.keys())
        to_create = desired_ids - existing_ids
        to_remove = existing_ids - desired_ids
        to_update = desired_ids & existing_ids

        # Apply lifecycle changes
        sync_dag_lifecycle(existing_factories, desired_factories, log_message)

        total_changes = len(to_remove) + len(to_create) + len(to_update)
        log_message(f"📊 TOTAL FACTORY, CQRS & DC RECONCILE CHANGES: {total_changes}")
        log_message(f"📊 ACTIVE FACTORY, CQRS & DC RECONCILE DAGS: {len(desired_factories)}")

        return f"Factory, CQRS & DC Reconcile DAG lifecycle: -{len(to_remove)} +{len(to_create)} ~{len(to_update)} = {len(desired_factories)} active"

    except Exception as e:
        error_msg = f"❌ Error creating factory & CQRS DAGs: {e}"
        log_message(error_msg)
        if is_task_execution:
            raise Exception(f"Factory & CQRS DAG creation failed: {str(e)}")
        else:
            # During module import, don't raise - just log and continue
            return f"Factory & CQRS DAG creation failed: {str(e)}"

# Create dynamic DAGs during module import
# Use parsing context to determine execution mode:
#   - dag_id is None → Discovery mode (scheduler parsing) → create ALL DAGs, verbose logging
#   - dag_id is set → Task execution → create ONLY the target DAG (optimization for Airflow 3.x)
#
# Airflow 3.x SDK task runner re-imports the Python file for every task execution.
# Without optimization, this would recreate ALL dynamic DAGs (potentially 1000+) for each task.
# The optimization parses the target dag_id and creates only that single DAG.
_parsing_context = get_parsing_context()
_target_dag_id = _parsing_context.dag_id
_is_discovery_mode = _target_dag_id is None
_silent_mode = not _is_discovery_mode  # Set module-level flag for all functions to use

if _is_discovery_mode:
    # Discovery mode: scheduler is parsing DAG files, create dynamic DAGs for this shard
    if _num_shards > 1:
        print(f"📦 Shard {_shard_number}/{_num_shards}: Loading DAGs where dag_hash % {_num_shards} = {_shard_number}")
    try:
        initial_factory_dags = create_factory_dags_from_database(silent=False)
        print(f"🚀 Factory & CQRS DAG creation during discovery: {initial_factory_dags}")
    except Exception as e:
        print(f"Warning: Failed to load initial Factory & CQRS DAGs during discovery: {e}")
else:
    # Task execution mode: SDK task runner is importing the file to run a specific task
    # OPTIMIZATION: Only create the single DAG needed instead of all dynamic DAGs
    try:
        _single_dag_result = _create_only_target_dag(_target_dag_id)
        if _single_dag_result is not None:
            # SUCCESS: Single DAG was created - log this prominently
            _logger.info(f"🎯 SINGLE-DAG OPTIMIZATION: {_single_dag_result}")
            print(f"🎯 SINGLE-DAG OPTIMIZATION: {_single_dag_result}")
        else:
            # Fallback: if single-DAG creation failed, create all DAGs silently
            _logger.info(f"⚠️ Single DAG creation returned None for {_target_dag_id}, falling back to full creation")
            print(f"⚠️ Single DAG creation returned None for {_target_dag_id}, falling back to full creation")
            create_factory_dags_from_database(silent=True)
    except Exception as e:
        # Silent fallback: try full creation if single-DAG creation fails
        _logger.info(f"⚠️ Single DAG creation failed for {_target_dag_id}: {e}, falling back to full creation")
        print(f"⚠️ Single DAG creation failed for {_target_dag_id}: {e}, falling back to full creation")
        try:
            create_factory_dags_from_database(silent=True)
        except Exception as e2:
            _logger.warning(f"Fallback full DAG creation also failed: {e2}")

# Factory & CQRS DAG Creation Task - Synchronize factory and CQRS DAGs from database configurations
# Airflow 3.x: provide_context removed - context always provided
factory_creation_task = PythonOperator(
    task_id='create_factory_and_cqrs_dags',
    python_callable=create_factory_dags_from_database,
    dag=dag
)

# End task
end_task = EmptyOperator(
    task_id='end_infrastructure_tasks',
    dag=dag
)

# Define task dependencies
# MERGE task runs first to generate infrastructure and populate factory DAG table
start_task >> merge_task  # type: ignore

# Factory creation runs after merge to read the populated factory DAG table
merge_task >> factory_creation_task  # type: ignore

# Security and metrics can run in parallel after factory creation
# Reconcile views are now handled by dedicated DC reconcile DAGs
factory_creation_task >> [apply_security_task, metrics_collector_task]  # type: ignore

# Table removal runs after security is applied
apply_security_task >> table_removal_task  # type: ignore

# All tasks complete before end
[metrics_collector_task, table_removal_task] >> end_task  # type: ignore