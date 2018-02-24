from importlib import import_module
from typing import List, ClassVar, Type

import tfci.daemon
import tfci.opcode


class Plugin:
    name = 'default'
    version = '0.0.1'
    description = 'default plugin'

    def __init__(self, *args, **kwargs):
        pass

    def startup(self):
        pass

    def get_daemons(self) -> List[Type['tfci.daemon.Daemon']]:
        return []

    def get_opcodes(self) -> List[Type['tfci.opcode.OpcodeDef']]:
        opcodes_name, _ = self.__class__.__module__.rsplit('.', 1)
        opcodes_name += '.opcodes'
        try:
            mod = import_module(opcodes_name)
        except ImportError:
            return []
        else:
            return tfci.opcode.OpcodeDef.find_module(mod)

    def teardown(self):
        pass
