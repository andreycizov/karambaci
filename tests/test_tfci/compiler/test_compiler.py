import os
import unittest

from typing import Tuple

import tfci_core.opcodes
from tfci.dsl.compiler import compiler_compile_text
from tfci.dsl.exception import CompilerException
from tfci.dsl.struct import ProgramDefinition
from tfci.opcode import OpcodeDef
from tfci.settings import TFException


def load_test_program(filename, supported_opcodes) -> Tuple[str, str, ProgramDefinition]:
    filename = os.path.join(os.path.dirname(__file__), filename)
    with open(filename) as f_in:
        text = f_in.read()

        return filename, text, compiler_compile_text(filename, text, supported_opcodes)


def load_opcodes(*mods):
    r = {}
    for mod in mods:
        for o in OpcodeDef.find_module(mod):
            try:
                r[o.name] = o()
            except:
                raise TFException(f'While {mod} {o.__module__} {o.__name__}')
    return r


class TestCompiler(unittest.TestCase):
    def test_label_redefinition(self):
        try:
            _, _, x = load_test_program(
                "dasm_label_redefinition.txt", load_opcodes(tfci_core.opcodes)
            )
        except CompilerException as e:
            self.assertEqual(e.reason, 'Redefinition of label `label_2`')
            self.assertEqual(e.lineno, 3)
            self.assertEqual(e.col, 1)

            self.assertEqual(e.subsequent.lineno, 2)
            self.assertEqual(e.subsequent.col, 1)

    def test_unsupported_opcode(self):
        try:
            _, _, x = load_test_program(
                "dasm_unsupported_opcode.txt", load_opcodes(tfci_core.opcodes)
            )
        except CompilerException as e:
            self.assertEqual(e.reason, 'Unsupported opcode `unsupported`')
            self.assertEqual(e.lineno, 1)
            self.assertEqual(e.col, 10)


