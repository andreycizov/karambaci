from argparse import ArgumentParser

from tfci.daemon import Daemon
from tfci.db.manager import EntityManager
from tfci_http.struct import RouteDef
from tfci_std.struct import FrozenThreadContext


class RouteManager(EntityManager):
    entity = RouteDef.__name__
    model = RouteDef

    @classmethod
    def arguments_add(cls, args: ArgumentParser):
        super().arguments_add(args)

        args.add_argument(
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

        args.add_argument(
            dest='route',
        )

        args.add_argument(
            dest='frz_id',
        )

    def action_add(self, id, method, route, frz_id, **kwargs):
        route = RouteDef.new(
            id,
            method,
            route,
            frz_id
        )

        check, succ = route.create(self.db)

        check_2 = FrozenThreadContext.exists(self.db, frz_id)

        return [check, check_2], [succ]


class ConfDaemon(Daemon):
    name = 'conf'
    description = 'HTTP server configurator'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def arguments(cls, args: ArgumentParser):
        RouteManager.arguments(args)

    def run(self):
        RouteManager(self.db).action(**self.kwargs)
