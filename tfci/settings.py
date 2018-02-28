import logging
from importlib import import_module
import random
from typing import List, Iterator, Tuple, AnyStr, Type

import etcd3

import tfci
import tfci.opcode
import tfci.daemon
from tfci.dsm.rt import OpcodeDefinition

logger = logging.getLogger(__name__)


class TFException(Exception):
    pass


class Settings:
    def __init__(self, plugins=None, etcd=None):
        if plugins is None:
            plugins = []

        import tfci.plugin

        self.plugins = plugins  # type: List[tfci.plugin.Plugin]
        self.plugins_by_name = {x.name: x for x in self.plugins}
        self.etcd = etcd

    def setup_logging(self):
        import sys
        import logging

        logger = logging.getLogger()
        ch = logging.StreamHandler(sys.stderr)

        format = logging.Formatter("[%(asctime)s][%(levelname)s][%(name)s]\t%(message)s")
        ch.setFormatter(format)
        ch.setLevel(logging.NOTSET)
        logger.addHandler(ch)

        logger.setLevel(logging.DEBUG)

    @classmethod
    def from_module(cls, module):
        try:
            r = import_module(module)
        except ModuleNotFoundError as e:
            raise TFException(f'Could not load settings from `{module}`')

        try:
            return getattr(r, 'OPTS')
        except AttributeError:
            raise TFException(f'Could not load `OPTS` from `{module}`')

    def get_plugin_module(self, plugin, module):
        try:
            return import_module(f'{plugin}.{module}')
        except ModuleNotFoundError as e:
            return None

    def get_plugin(self, name):
        if name in self.plugins_by_name:
            return self.plugins_by_name[name]
        else:
            raise TFException(f'Could not find plugin `{name}`')

    def get_daemons(self) -> Iterator[Tuple[AnyStr, Type['tfci.daemon.Daemon']]]:
        daemons = []

        for plugin in self.plugins:
            for daemon in plugin.get_daemons():
                daemons.append((f'{plugin.name}.{daemon.name}', daemon))

        return daemons

    def get_opcodes(self) -> OpcodeDefinition:
        opcodes = {}

        for plugin in self.plugins:
            for opcode in plugin.get_opcodes():
                if opcode.name in opcodes:
                    raise TFException(f'`{opcode.name}` is already defined in {opcodes[opcode.name]}')

                opcodes[opcode.name] = opcode

        return opcodes

    def get_logger(self):
        import logging
        return logging.getLogger()

    def get_db(self) -> etcd3.Etcd3Client:
        x = random.choice(self.etcd)

        self.get_logger().getChild('db').debug(f'Selected {x}')

        kwargs = dict(
            host=x['h'],
            port=x['p'],
        )

        if 'ca' in x:
            kwargs.update(dict(
                ca_cert=x['ca'],
                cert_cert=x['cert'],
                cert_key=x['key'],
            ))
        elif 'u' in x:
            kwargs.update(dict(
                user=x['u'],
                password=x['p'],
            ))

        return etcd3.client(
            timeout=x.get('t', 10),
            **kwargs,
        )
