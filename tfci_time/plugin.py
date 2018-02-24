from typing import List, Type

from tfci.daemon import Daemon
from tfci.plugin import Plugin
from tfci_time.daemons import TimeDaemon


class TimePlugin(Plugin):
    name = 'time'
    version = '0.0.1'
    description = ''

    def startup(self):
        pass

    def get_daemons(self) -> List[Type[Daemon]]:
        return [TimeDaemon]

    def teardown(self):
        pass
