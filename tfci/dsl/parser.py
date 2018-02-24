import pyparsing as pp

from tfci.dsl.ast import Location, ConstantType, Constant, Label, Opcode, Identifier, Empty, Map, Command, Comment, \
    Lines

pp.ParserElement.setDefaultWhitespaceChars(" \t")

eol = pp.LineEnd()
eol.setName('EOL')

string_constant = pp.quotedString()
string_constant.addParseAction(pp.removeQuotes)
string_constant.addParseAction(lambda s, loc, toks: Constant(ConstantType.String, toks[0], Location(loc)))

identifier_chars = pp.alphanums + '_.-'

key_path_chars = identifier_chars + ':/'

identifier = pp.ZeroOrMore('^') + pp.Word(identifier_chars)


def map_identifier(s, loc, toks):
    return Identifier(len(toks) - 1, toks[-1], Location(loc))


identifier.addParseAction(map_identifier)

address_constant = '@' + pp.Word(key_path_chars)
address_constant.addParseAction(lambda s, loc, toks: Constant(ConstantType.Address, toks[1], Location(loc)))

integer_constant = '%' + pp.Word(identifier_chars)
integer_constant.addParseAction(lambda s, loc, toks: Constant(ConstantType.Integer, int(toks[1]), Location(loc)))

label = pp.Word(identifier_chars) + ':'
label.addParseAction(lambda s, loc, toks: Label(toks[0], loc=Location(loc)))

op = pp.Word(identifier_chars)
op.addParseAction(lambda s, loc, toks: Opcode(toks[0].lower(), loc=Location(loc)))

arg = identifier | string_constant | address_constant | integer_constant

kw_arg = identifier + '=' + arg
kw_arg.addParseAction(lambda s, loc, toks: Map(toks[0], toks[2], loc=Location(loc)))

args = pp.delimitedList(kw_arg | arg, pp.White('\t '))
args = pp.Optional(args)
kw_arg.addParseAction(lambda s, loc, toks: toks[0] if len(toks) else [])

empty = pp.empty()
empty.addParseAction(lambda s, loc, toks: Empty('', Location(loc)))

comment = '#' + pp.restOfLine()
comment.addParseAction(lambda s, loc, toks: Comment(toks[1], Location(loc)))


def map_command(s, loc, toks):
    if isinstance(toks[0], Label):
        return Command(toks[0], toks[1], toks[2:-1], toks[-1], Location(loc))
    else:
        return Command(Label(None, Location(loc)), toks[0], toks[1:-1], toks[-1], Location(loc))


command = pp.Optional(label) + op + args + pp.Optional(comment, default=None)
command.addParseAction(map_command)
command.setName('COMMAND')

line = comment | command | label | empty

lines = pp.delimitedList(line, eol)
lines.addParseAction(lambda s, loc, toks: Lines(toks[:], Location(loc)))
