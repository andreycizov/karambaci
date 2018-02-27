from typing import List, Union, NamedTuple, Optional, Dict

from tfci.dsl.ast import Constant, Identifier, Map, Location

OpcodeArg = Union[Identifier, Constant, Map]
OpcodeArgs = List[OpcodeArg]


class ProgramDefinitionItem(NamedTuple):
    opcode: str
    args: OpcodeArgs
    next_label: Optional[str]
    loc: Location

    def __repr__(self):
        return f'({self.opcode} {self.args} {self.next_label})'


ProgramDefinition = Dict[str, ProgramDefinitionItem]
