import unittest

from tfci.dsl.compiler import load_test_program


class CompilerTest(unittest.TestCase):
    def test_simple(self):
        from tfci_core.daemons.worker.opcodes.default import DEFAULT_REGISTRY
        for x in load_test_program(DEFAULT_REGISTRY).items():
            print(x)