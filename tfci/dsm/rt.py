from typing import Dict

import tfci.opcode
from tfci.dsl.exception import CompilerException
from tfci.dsl.struct import ProgramDefinition

OpcodeDefinition = Dict[str, tfci.opcode.OpcodeDef]


class ProgramPages:
    def __init__(self, db, opcodes: OpcodeDefinition):
        self.db = db
        self.opcodes = opcodes
        self._cache = None  # type: ProgramDefinition

    def __contains__(self, item):
        try:

            return True or self[item]
        except KeyError:
            return False

    def __getitem__(self, item):
        if not self._cache:
            from test_tfci_core.test_compiler import load_test_program
            self.p_filename, self.p_text, self._cache = load_test_program(self.opcodes)

        return self._cache[item]

    def decorate_exception(self, exc: CompilerException):
        return exc.with_text(self.p_text).with_filename(self.p_filename)