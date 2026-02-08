"""
Copyright (c) 2026 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.
"""

import unittest
from datasurface.dsl import Ecosystem, DataPlatform, EcosystemPipelineGraph, PlatformPipelineGraph
from datasurface.validation import ValidationTree
from datasurface.model import loadEcosystemFromEcoModule
from typing import Any, Optional


class TestEcosystem(unittest.TestCase):
    def test_createEcosystem(self):
        ecosys: Optional[Ecosystem]
        ecoTree: Optional[ValidationTree]
        ecosys, ecoTree = loadEcosystemFromEcoModule(".")  # Check like a PR would first.
        self.assertIsNotNone(ecosys)
        self.assertIsNotNone(ecoTree)
        assert ecoTree is not None
        assert ecosys is not None
        if ecoTree.hasErrors():
            print("Ecosystem validation failed with errors:")
            ecoTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if ecoTree.hasWarnings():
                print("Note: There are some warnings:")
                ecoTree.printTree()

        ecosys, ecoTree = loadEcosystemFromEcoModule(".", "demo")  # demo is the runtime environment name
        self.assertIsNotNone(ecosys)
        self.assertIsNotNone(ecoTree)
        assert ecoTree is not None
        assert ecosys is not None
        if ecoTree.hasErrors():
            print("Ecosystem validation failed with errors:")
            ecoTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if ecoTree.hasWarnings():
                print("Note: There are some warnings:")
                ecoTree.printTree()
        vTree: ValidationTree = ecosys.lintAndHydrateCaches()
        if (vTree.hasErrors()):
            print("Ecosystem validation failed with errors:")
            vTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if vTree.hasWarnings():
                print("Note: There are some warnings:")
                vTree.printTree()
        scd2_dp: DataPlatform[Any] = ecosys.getDataPlatformOrThrow("SCD2")  # type: ignore
        self.assertIsNotNone(scd2_dp)
        graph: EcosystemPipelineGraph = ecosys.getGraph()
        self.assertIsNotNone(graph)
        scd2_root: Optional[PlatformPipelineGraph] = graph.roots.get(scd2_dp.name)
        self.assertIsNotNone(scd2_root)


if __name__ == "__main__":
    unittest.main()
