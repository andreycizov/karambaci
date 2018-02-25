from typing import Type, List

import tfci.daemon
from tfci.plugin import Plugin
from tfci_std.daemons import ConfDaemon


class StdPlugin(Plugin):
    name = 'std'
    version = '0.0.1'
    description = 'std lib'

    def get_daemons(self) -> List[Type['tfci.daemon.Daemon']]:
        return [ConfDaemon]
