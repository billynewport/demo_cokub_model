"""
Copyright (c) 2026 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.
"""

from datasurface.dsl import GovernanceZone, Team, EnvironmentMap, Datastore, Dataset
from datasurface.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.containers import SQLSnapshotIngestion, SQLCDCIngestion
from datasurface.dsl import EnvRefDataContainer, IngestionConsistencyType
from datasurface.triggers import CronTrigger
from datasurface.security import Credential, CredentialType
from datasurface.documentation import PlainTextDocumentation
from datasurface.policy import SimpleDC, SimpleDCTypes
from datasurface.types import VarChar, Date
from datasurface.containers import HostPortPair, PostgresDatabase, SQLServerDatabase
from datasurface.dsl import ProductionStatus
from datasurface.keys import LocationKey
from db_constants import MERGE_HOST, SQLSERVER_HOST


def createProducerTeam(gz: GovernanceZone) -> None:
    producerTeam: Team = gz.getTeamOrThrow("producerTeam")
    producerTeam.add(
        EnvironmentMap(
            "demo",
            dataContainers={
                frozenset(["customer_db"]): PostgresDatabase(
                    "CustomerDB",  # Model name for database
                    hostPort=HostPortPair(MERGE_HOST, 5432),
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    productionStatus=ProductionStatus.NOT_PRODUCTION,
                    databaseName="customer_db"  # Database name
                ),
                frozenset(["customer_db_sqlserver"]): SQLServerDatabase(
                    "CustomerDB",  # Model name for database
                    hostPort=HostPortPair(SQLSERVER_HOST, 1433),
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    productionStatus=ProductionStatus.NOT_PRODUCTION,
                    databaseName="customer_db"  # Database name
                )
            },
            dtReleaseSelectors=dict(),
            dtDockerImages=dict()
        ),
        Datastore(
            "CustomerDB",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                EnvRefDataContainer("customer_db"),
                CronTrigger("Every 5 minute", "*/5 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("customer-source-credential", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
                ),
            datasets=[
                Dataset(
                    "customers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("firstname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("lastname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("dob", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("email", VarChar(100)),
                            DDLColumn("phone", VarChar(100)),
                            DDLColumn("primaryaddressid", VarChar(20)),
                            DDLColumn("billingaddressid", VarChar(20))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Customer")]
                ),
                Dataset(
                    "addresses",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customerid", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("streetname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("city", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("state", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("zipcode", VarChar(30), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")]
                )
            ]
        ),
        Datastore(
            "CustomerDB_SQLServer",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLCDCIngestion(
                EnvRefDataContainer("customer_db_sqlserver"),
                CronTrigger("Every 1 minute", "*/1 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("customer-sqlserver-source-credential", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
                ),
            datasets=[
                Dataset(
                    "customers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("firstname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("lastname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("dob", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("email", VarChar(100)),
                            DDLColumn("phone", VarChar(100)),
                            DDLColumn("primaryaddressid", VarChar(20)),
                            DDLColumn("billingaddressid", VarChar(20))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Customer")]
                ),
                Dataset(
                    "addresses",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customerid", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("streetname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("city", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("state", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("zipcode", VarChar(30), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")]
                )
            ]
        )
    )
