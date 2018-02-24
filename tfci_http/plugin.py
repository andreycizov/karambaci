from typing import List, Type

from tfci.daemon import Daemon
from tfci.plugin import Plugin
from tfci_http.daemons import HTTPDaemon, ConfDaemon


class HTTPPlugin(Plugin):
    name = 'http'
    version = '0.0.1'
    description = ''

    def startup(self):
        pass

    def get_daemons(self) -> List[Type['Daemon']]:
        return [HTTPDaemon, ConfDaemon]

    def teardown(self):
        pass
