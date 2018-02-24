# a deamon that listens on a specific port and fires up tasks related to the received requests.
import multiprocessing
import time
from argparse import ArgumentParser
from typing import Dict
from uuid import uuid4

from tfci.daemon import Daemon
from tfci.settings import TFException
from tfci_core.daemons.db_util import watch_range_queue, queue_try, Ev, RangeEvent
from tfci_http.const import http_sub_key
from tfci_http.server import ServerProcess
from tfci_http.struct import RouteDef


class ConfDaemon(Daemon):
    name = 'conf'
    description = 'HTTP server configurator'

    def __init__(self, utility, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.utility = utility
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def arguments(cls, args: ArgumentParser):
        utility_args = args.add_subparsers(
            title="utility",
            dest="utility",
            help="select the utility to run",
            metavar="UTIL",
        )
        utility_args.required = True

        util_list = utility_args.add_parser('list', help=f'list created routes')
        util_add = utility_args.add_parser('add', help=f'add a route')
        util_rm = utility_args.add_parser('rm', help=f'remove a route')

        util_rm.add_argument(
            dest='id',
        )

        def stack_var(v):
            a, b = str(v).split('=', 1)

            return a, b

        util_add.add_argument(
            '-v',
            dest='stack_const',
            action='append',
            default=[],
            type=stack_var,
            help="Add a constant value on the stack"
        )

        util_add.add_argument(
            '-e',
            dest='stack_exec',
            action='append',
            default=[],
            type=stack_var,
            help="Add a result value of a python expression on the stack",
        )

        util_add.add_argument(
            '-X',
            '--method',
            dest='method',
            action='append',
            required=True,
            default=[],
            type=lambda x: str(x).upper(),
            choices=['PUT', 'POST', 'GET', 'HEAD'],
            help="HTTP methods which would trigger the callback",
        )

        util_add.add_argument(
            '-I',
            dest='id',
            required=False,
            default=None,
        )

        util_add.add_argument(
            dest='route',
        )

        util_add.add_argument(
            dest='ep',
        )

    def run(self):
        if self.utility == 'list':
            print('ROUTES')
            for k, v in RouteDef.load_all(self.db).items():
                print(k, v)
        elif self.utility == 'add':
            id = self.kwargs['id']
            method = self.kwargs['method']
            stack_const = self.kwargs['stack_const']
            stack_exec = self.kwargs['stack_exec']

            route = self.kwargs['route']
            ep = self.kwargs['ep']

            def check_in_stack(k):
                if k in stack:
                    raise KeyError(f'{k} defined twice')

            stack = {}

            for k, v in stack_const:
                check_in_stack(k)
                stack[k] = v

            for k, v in stack_exec:
                check_in_stack(k)

                try:
                    v = eval(compile(v, filename='<string>', mode='eval'))
                    stack[k] = v
                except:
                    self.settings.get_logger().error(f'While evaluating {k}={v}')
                    raise

            if id is None:
                id = uuid4().hex
                print('Generated a new ID', id)

            route = RouteDef.new(
                id,
                method,
                route,
                ep,
                stack
            )

            check, create = route.create(self.db)

            ok, _ = self.db.transaction(compare=[check], success=[create], failure=[])

            if ok:
                print('OK')
            else:
                print('Failure')
        elif self.utility == 'rm':
            self.db.delete(RouteDef.key_fn(self.kwargs['id']))
        else:
            raise NotImplementedError(self.utility)


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

        SUB_PREFIX = http_sub_key('')

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
