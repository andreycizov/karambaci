import os
from typing import Dict, Type, Tuple

from pyparsing import ParseException

from tfci.dsl.exception import CompilerException
from tfci.dsl.parser import lines
from tfci.dsl.ast import Location, Label, Opcode, Empty, Command, Comment, Lines
from tfci.dsl.struct import ProgramDefinitionItem, ProgramDefinition
import tfci.opcode


def compiler_first_pass(text) -> Lines:
    try:
        xx = lines.parseString(text, parseAll=True)[0]
        return xx
    except ParseException as e:
        raise CompilerException(e.loc, f'Tokenizer error: "{e}"') from None


HALT_LABEL = '$halt'


def compiler_second_pass(lines: Lines) -> Lines:
    rtn = []

    i = 0
    for i, x in enumerate(lines.items):
        if isinstance(x, Empty):
            pass
        elif isinstance(x, Command):

            if x.label.name is None:
                new_label = Label(f'${i}', x.loc)
            else:
                new_label = x.label

            rtn.append(Command(new_label, x.opcode, x.args, None, loc=x.loc))
        elif isinstance(x, Label):
            rtn.append(Command(x, Opcode('nop', loc=x.loc), [], None, loc=x.loc))
        elif isinstance(x, Comment):
            pass
        else:
            raise NotImplementedError(f'{x}')

    last_loc = Location(99999999999)
    rtn.append(Command(Label(HALT_LABEL, loc=last_loc), Opcode('hlt', loc=last_loc), [], None, loc=last_loc))

    return Lines(rtn, loc=lines.loc)


SupportedOpcodes = Dict[str, Type[tfci.opcode.OpcodeDef]]


def compiler_third_pass(lines: Lines, supported_opcodes: SupportedOpcodes) -> Lines:
    rtn = []

    ids_defined = {}  # type: Dict[str, Command]

    for i, x in enumerate(lines.items):
        x  # type: Command

        # todo: check that all arguments referencing addresses are existing.

        if x.label.name in ids_defined:
            prev = ids_defined[x.label.name]
            raise CompilerException(
                x.label.loc,
                reason=f"Redefinition of label `{x.label.name}`",
                subsequent=CompilerException(
                    prev.label.loc,
                    "Location of the previous definition"
                )
            )

        ids_defined[x.label.name] = x

        if x.opcode.name in supported_opcodes:
            supported_opcodes[x.opcode.name]().check(x)
        else:
            raise CompilerException(
                x.opcode.loc,
                reason=f"Unsupported opcode `{x.opcode.name}`",
            )

        rtn.append(x)

    return Lines(rtn, loc=lines.loc)


def compiler_program_pass(lines: Lines) -> ProgramDefinition:
    r = {}  # type: ProgramDefinition
    for x, nxt_x in zip(lines.items[:-1], lines.items[1:]):
        r[x.label.name] = ProgramDefinitionItem(x.opcode.name, x.args, nxt_x.label.name, x.loc)

    for x in lines.items[-1:]:
        r[x.label.name] = ProgramDefinitionItem(x.opcode.name, x.args, None, x.loc)
    return r


def compiler_compile_text(filename, text, supported_opcodes) -> ProgramDefinition:
    try:
        xx = compiler_first_pass(text)
        xx = compiler_second_pass(xx)
        xx = compiler_third_pass(xx, supported_opcodes)
        xx = compiler_program_pass(xx)

        return xx
    except CompilerException as e:
        e = e.with_filename(filename).with_text(text)
        raise e from None


def load_test_program(supported_opcodes) -> Tuple[str, str, ProgramDefinition]:
    filename = os.path.join(os.path.dirname(__file__), 'example.txt')
    with open(filename) as f_in:
        text = f_in.read()

        return filename, text, compiler_compile_text(filename, text, supported_opcodes)

