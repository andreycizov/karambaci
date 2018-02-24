from typing import List, ClassVar, Type

from tfci.daemon import Daemon
from tfci.plugin import Plugin
from tfci_core.daemons.watcher import WatcherDaemon
from tfci_core.daemons.worker.daemon import WorkerDaemon, JobsDaemon


class CorePlugin(Plugin):
    name = 'core'
    version = '0.0.1'
    description = 'core'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def startup(self):
        pass

    def get_opcodes(self) -> List['tfci.opcode.SysOpcodeDef']:
        return super().get_opcodes()

    def get_daemons(self) -> List[Type[Daemon]]:
        return [WorkerDaemon, JobsDaemon, WatcherDaemon]

    def teardown(self):
        pass
