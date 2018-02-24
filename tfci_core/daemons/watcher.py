import time

from tfci.daemon import Daemon
from tfci_core.const import JOBS_THREAD, JOBS_LOCK, JOBS_STACK
from tfci_core.daemons.db_util import watch_range


class WatcherDaemon(Daemon):
    name = 'watcher'
    version = '0.0.1'
    description = 'task queue support'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def watch_restart(self):

        # watch_id1 = watch_range(self.db, JOBS_THREAD % ('',), self.log_item)
        # watch_id2 = watch_range(self.db, JOBS_LOCK % ('',), self.log_item)
        # watch_id3 = watch_range(self.db, JOBS_STACK % ('',), self.log_item)
        watch_id3 = watch_range(self.db, '/', self.log_item)
        watch_daemons = watch_range(self.db, f'/daemons/{self.name}/', self.log_item)

    # def task_handle(self, task_id, task):
    #

    def log_item(self, prefix, item):
        self.settings.get_logger().info(f'Logged: {item}')

    def run(self):
        self.watch_restart()

        for y, x in self.db.get_all():
            print(x.key, y)
            # key = x.key.decode()
            # if key.startswith('/jobs/') or key.startswith('/locks'):
            #     self.db.delete(x.key)

        while True:
            time.sleep(1)

            self.lease_renew(False)

    def teardown(self):
        super().teardown()
