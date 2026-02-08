"""
Copyright (c) 2026 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.
"""

from datasurface.dsl import (
    GovernanceZone, Team, DatasetGroup, DatasetSink, Workspace, DataPlatformManagedDataContainer,
    WorkspacePlatformConfig, ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency)
from datasurface.documentation import PlainTextDocumentation


def createConsumerTeam(gz: GovernanceZone) -> None:
    team: Team = gz.getTeamOrThrow("consumerTeam")
    team.add(
        Workspace(
            "Consumer1",
            DataPlatformManagedDataContainer("Consumer1 container"),
            PlainTextDocumentation("Workspace to consume the datasets in CustomerDB datastore using SCD2"),
            DatasetGroup(
                "SCD2_DSG",
                sinks=[
                    DatasetSink("CustomerDB", "customers"),
                    DatasetSink("CustomerDB", "addresses")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.SCD2,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                )
            )
        )
    )
