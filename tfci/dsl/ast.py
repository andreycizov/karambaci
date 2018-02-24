from enum import Enum
from typing import NamedTuple, Any, Optional, Union, List, Tuple


class Location(NamedTuple):
    loc: int

    def __repr__(self):
        return f'{self.loc}'


class ConstantType(Enum):
    String = 'STRING'
    Address = 'ADDRESS'
    Integer = 'INTEGER'


class Constant(NamedTuple):
    type: ConstantType
    value: Any
    loc: Location

    def __repr__(self):
        if self.type == ConstantType.String:
            return f'"{self.value}"'
        elif self.type == ConstantType.Address:
            return f'@{self.value}'
        elif self.type == ConstantType.Integer:
            return f'%{self.value}'
        else:
            raise NotImplementedError(f'{self.value} {self.type}')


class Label(NamedTuple):
    name: Optional[str]
    loc: Location

    def __repr__(self):
        return f'{self.name}' if self.name else ''


class Opcode(NamedTuple):
    name: str
    loc: Location

    def __repr__(self):
        return f'{self.name.lower()}'


class Identifier(NamedTuple):
    level: int
    name: str
    loc: Location

    def __repr__(self):
        return f'${self.name}'


class Empty(NamedTuple):
    noop: str
    loc: Location

    def __repr__(self):
        return f''


class Map(NamedTuple):
    identifier: Identifier
    to: Union[Constant, Identifier]
    loc: Location

    def __repr__(self):
        a = repr(self.identifier)
        b = repr(self.to)
        return f'{a}={b}'


class Command(NamedTuple):
    label: Label
    opcode: Opcode
    args: List[Union[Identifier, Constant, Tuple[Identifier, Constant]]]
    comment: Optional['Comment']
    loc: Location

    def __repr__(self):
        label = '' if not repr(self.label) else repr(self.label) + ': '
        args = ' '.join(repr(x) for x in self.args)
        args = ' ' + args if args else ''
        comment = '' if self.comment is None else ' ' + repr(self.comment)
        return f'{label}{self.opcode}{args}{comment}'


class Comment(NamedTuple):
    text: str
    loc: Location

    def __repr__(self):
        return '#' + self.text


class Lines(NamedTuple):
    items: List[Union[Comment, Command, Empty]]
    loc: Location