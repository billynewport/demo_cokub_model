"""
// Copyright (c) 2026 William Newport
// SPDX-License-Identifier: BUSL-1.1

This is a starter datasurface repository. It defines a simple Ecosystem using YellowDataPlatform with Live and Forensic modes.
It will generate 2 pipelines, one with live records only and the other with full milestoning.
"""

from datasurface.dsl import InfrastructureVendor, InfrastructureLocation, Ecosystem, CloudVendor, RuntimeDeclaration
from datasurface.security import Credential, CredentialType
from datasurface.documentation import PlainTextDocumentation
from datasurface.repos import GitHubRepository
from rte_demo import createDemoRTE

GIT_REPO_OWNER: str = "billynewport"
GIT_REPO_NAME: str = "demo_cokub_model"


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
        liveRepo=GitHubRepository(f"{GIT_REPO_OWNER}/{GIT_REPO_NAME}", "main", credential=git)
    )
    # Define the demo RTE
    createDemoRTE(ecosys)
    return ecosys
