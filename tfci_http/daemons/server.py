import multiprocessing
import time
from argparse import ArgumentParser
from typing import Dict

from tfci.daemon import Daemon
from tfci.settings import TFException
from tfci.db.db_util import RangeEvent, Ev, watch_range_queue, queue_try
from tfci_http.server import ServerProcess
from tfci_http.struct import RouteDef


class HTTPDaemon(Daemon):
    name = 'server'
    description = 'HTTP server bindings'

    def __init__(self, host, port, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.manager = multiprocessing.Manager()

        self.p = ServerProcess(self.ident, (host, port))
        self.route_queue = self.manager.Queue()
        self.routes = {}  # type: Dict[str, RouteDef]
        self.watches = []

    @classmethod
    def arguments(cls, args: ArgumentParser):
        args.add_argument(
            '-H',
            '--host',
            dest='host',
            default='127.0.0.1'
        )

        args.add_argument(
            '-P',
            '--port',
            dest='port',
            type=int,
            default=8080
        )

    def routes_updated(self, ident, ev: RangeEvent):
        if ev.event == Ev.Delete:
            self.settings.get_logger().debug(f'route `{ident}` DELETED')
            del self.routes[ident]
        elif ev.event == Ev.Put:
            self.routes[ident] = RouteDef.deserialize(ident, ev.version, ev.body)
            self.settings.get_logger().debug(f'route `{ident}` CHANGED')

    def event_queue(self):
        self.route_queue.get()

    def restart(self):
        self.settings.get_logger().debug(f'> Restarting... Here\'s loaded routes:')
        for k, v in self.routes.items():
            self.settings.get_logger().debug(f'\t{k} {v}')
        self.settings.get_logger().debug(f'< End loaded routes')
        if self.p.running:
            self.p.stop()
        self.p.start(self.settings, self.routes)

    def run(self):
        self.routes = RouteDef.load_all(self.db)


        SUB_PREFIX = RouteDef.key_fn('')

        self.watches.append(watch_range_queue(self.route_queue, self.db, SUB_PREFIX))

        self.restart()

        while True:
            while self.lease_wait_max > 0:
                is_updated = False
                for x in queue_try(self.route_queue):
                    if x.prefix == SUB_PREFIX:
                        self.routes_updated(x.ident, x)
                        is_updated = True
                if is_updated:
                    self.restart()

                if self.p.p.poll() is not None:
                    raise TFException('Child process must be running')

                time.sleep(1)
            self.lease_renew(False)

    def teardown(self):
        for w in self.watches:
            self.db.cancel_watch(w)
        super().teardown()