from typing import List, Type

from tfci.daemon import Daemon
from tfci.plugin import Plugin
from tfci_docker.daemons import ConfDaemon


class DockerPlugin(Plugin):
    name = 'docker'
    version = '0.0.1'
    description = ''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def startup(self):
        pass

    def get_daemons(self) -> List[Type[Daemon]]:
        return [ConfDaemon]

    def teardown(self):
        pass
