"""
Copyright (c) 2026 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.
"""

from datasurface.dsl import Ecosystem, GovernanceZone, TeamDeclaration
from datasurface.repos import GitHubRepository
from repo_constants import GIT_REPO_OWNER, GIT_REPO_NAME
from producer_team import createProducerTeam
from consumer_team import createConsumerTeam


def createDemoGZ(ecosys: Ecosystem) -> None:
    gz: GovernanceZone = ecosys.getZoneOrThrow("demo_gz")
    gz.add(TeamDeclaration(
        "producerTeam",
        GitHubRepository(f"{GIT_REPO_OWNER}/{GIT_REPO_NAME}", "producer_team_edit", credential=ecosys.owningRepo.credential)
        ))
    gz.add(TeamDeclaration(
        "consumerTeam",
        GitHubRepository(f"{GIT_REPO_OWNER}/{GIT_REPO_NAME}", "consumer_team_edit", credential=ecosys.owningRepo.credential)
        ))
    createProducerTeam(gz)
    createConsumerTeam(gz)
