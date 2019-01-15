import os
from typing import Tuple

from tfci.dsl.compiler import compiler_compile_text
from tfci.dsl.struct import ProgramDefinition


def load_test_program(filename, supported_opcodes) -> Tuple[str, str, ProgramDefinition]:
    filename = os.path.join(os.path.dirname(__file__), filename)
    with open(filename) as f_in:
        text = f_in.read()

        return filename, text, compiler_compile_text(filename, text, supported_opcodes)