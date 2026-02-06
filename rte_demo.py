"""
Copyright (c) 2026 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.

This is a starter datasurface repository. It defines a simple Ecosystem using YellowDataPlatform with SCD2 modes. It
ingests data from a single source, using a Workspace to produce a masked version of that data and provides consumer Workspaces
to that data in the primary merge Postgres.

It will generate 1 pipelines and it supports full milestoning (SCD2).
"""

from datasurface.dsl import ProductionStatus, \
    RuntimeEnvironment, Ecosystem, PSPDeclaration, \
    DataMilestoningStrategy
from datasurface.keys import LocationKey
from datasurface.containers import HostPortPair, PostgresDatabase
from datasurface.security import Credential, CredentialType
from datasurface.documentation import PlainTextDocumentation
from datasurface.platforms.yellow import YellowDataPlatform, YellowPlatformServiceProvider
from datasurface.platforms.yellow.assembly import GitCacheConfig, YellowExternalAirflow3AndMergeDatabase
from datasurface.repos import VersionPatternReleaseSelector, GitHubRepository, ReleaseType, VersionPatterns

# Docker Desktop configuration
KUB_NAME_SPACE: str = "demo-cokub"
AIRFLOW_SERVICE_ACCOUNT: str = "airflow-worker"
MERGE_HOST: str = "postgres-co"
MERGE_DBNAME: str = "merge_db"


def createDemoPSP() -> YellowPlatformServiceProvider:
    # Kubernetes merge database configuration
    k8s_merge_datacontainer: PostgresDatabase = PostgresDatabase(
        "K8sMergeDB",  # Container name for Kubernetes deployment
        hostPort=HostPortPair(MERGE_HOST, 5432),
        locations={LocationKey("MyCorp:USA/NY_1")},  # Kubernetes cluster location
        productionStatus=ProductionStatus.NOT_PRODUCTION,
        databaseName=MERGE_DBNAME
    )

    git_config: GitCacheConfig = GitCacheConfig(
        enabled=True,
        access_mode="ReadWriteMany",
        storageClass="longhorn"
    )
    yp_assembly: YellowExternalAirflow3AndMergeDatabase = YellowExternalAirflow3AndMergeDatabase(
        name="Demo",
        namespace=KUB_NAME_SPACE,
        git_cache_config=git_config
    )

    psp: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Demo_PSP",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Demo PSP"),
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("postgres-demo-merge", CredentialType.USER_PASSWORD),
        yp_assembly=yp_assembly,
        merge_datacontainer=k8s_merge_datacontainer,
        pv_storage_class="longhorn",
        datasurfaceDockerImage="registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0",
        dataPlatforms=[
            YellowDataPlatform(
                "SCD2",
                doc=PlainTextDocumentation("SCD2 Yellow DataPlatform"),
                milestoneStrategy=DataMilestoningStrategy.SCD2,
                stagingBatchesToKeep=5
                )
        ]
    )
    return psp


def createDemoRTE(ecosys: Ecosystem) -> RuntimeEnvironment:
    assert isinstance(ecosys.owningRepo, GitHubRepository)

    psp: YellowPlatformServiceProvider = createDemoPSP()
    rte: RuntimeEnvironment = ecosys.getRuntimeEnvironmentOrThrow("demo")
    # Allow edits using RTE repository
    rte.configure(VersionPatternReleaseSelector(
        VersionPatterns.VN_N_N+"-demo", ReleaseType.STABLE_ONLY),
        [PSPDeclaration(psp.name, rte.owningRepo)],
        productionStatus=ProductionStatus.NOT_PRODUCTION)
    rte.setPSP(psp)
    return rte
