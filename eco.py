"""
Copyright (c) 2026 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.
"""

from datasurface.dsl import InfrastructureVendor, InfrastructureLocation, Ecosystem, CloudVendor, RuntimeDeclaration
from datasurface.security import Credential, CredentialType
from datasurface.documentation import PlainTextDocumentation
from datasurface.repos import GitHubRepository
from rte_demo import createDemoRTE
from datasurface.dsl import GovernanceZoneDeclaration
from repo_constants import GIT_REPO_OWNER, GIT_REPO_NAME
from demo_gz import createDemoGZ


def createEcosystem() -> Ecosystem:
    """This is a very simple test model with a single datastore and dataset.
    It is used to test the YellowDataPlatform. We are using a monorepo approach
    so all the model fragments use the same owning repository.

    Updated ecosystem documentation for testing workflow.
    """

    git: Credential = Credential("git", CredentialType.API_TOKEN)
    eRepo: GitHubRepository = GitHubRepository(f"{GIT_REPO_OWNER}/{GIT_REPO_NAME}", "main_edit", credential=git)

    ecosys: Ecosystem = Ecosystem(
        name="Demo",
        repo=eRepo,
        runtimeDecls=[
            RuntimeDeclaration("demo", GitHubRepository(f"{GIT_REPO_OWNER}/{GIT_REPO_NAME}", "demo_rte_edit", credential=git))
        ],
        infrastructure_vendors=[
            # Onsite data centers
            InfrastructureVendor(
                name="MyCorp",
                cloud_vendor=CloudVendor.PRIVATE,
                documentation=PlainTextDocumentation("Private company data centers - updated"),
                locations=[
                    InfrastructureLocation(
                        name="USA",
                        locations=[
                            InfrastructureLocation(name="NY_1")
                        ]
                    )
                ]
            )
        ],
        governance_zone_declarations=[
            GovernanceZoneDeclaration("demo_gz", GitHubRepository(f"{GIT_REPO_OWNER}/{GIT_REPO_NAME}", "demo_gz_edit", credential=git))
        ],
        liveRepo=GitHubRepository(f"{GIT_REPO_OWNER}/{GIT_REPO_NAME}", "main", credential=git)
    )
    # Define the demo RTE
    createDemoRTE(ecosys)
    # Define the demo GZ
    createDemoGZ(ecosys)
    return ecosys
