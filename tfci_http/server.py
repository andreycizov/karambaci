# given a definition of mappings between the paths
# serve requests
import setproctitle
import subprocess

from functools import partial
from typing import Optional, Dict

from bottle import Bottle
from bottle import response

from tfci.settings import Settings
from tfci.time import time_now
from tfci_core.daemons.db_util import watch_range_single, Ev
from tfci_core.daemons.generic.pool import argv_decode, argv_encode
from tfci_http.struct import RouteDef, Request, Reply


def bottle_server_process(ident, address, settings, routes):
    setproctitle.setproctitle(f'http-{ident}')

    address = argv_decode(address)
    settings = argv_decode(settings)  # type: Settings
    routes = argv_decode(routes)  # type: Dict[str, RouteDef]

    bottle_server(ident, address, settings, routes)


def bottle_server_callback(settings: Settings, route_def: RouteDef, *args, **kwargs):
    db = settings.get_db()

    stack = {**route_def.stack, **kwargs}

    time_start = time_now()
    ok = False
    body = {}

    try:
        r = Request.new()

        token = watch_range_single(db, Reply.key_fn(r.id))

        ok, r = Request.initiate(db, route_def, r, *args, **stack)

        if ok:
            ev = token.get(timeout=3, close=False)
            assert ev.event == Ev.Put, f'{r.key} {ev}'

            r = Reply.deserialize(r.key, ev.body)

            body.update(r.result)
    except TimeoutError:
        response.status = 408
        ok = False
    finally:
        time_end = time_now()

        time_total = time_end - time_start

        body['success'] = ok
        body['t'] = time_total.total_seconds()
    return body


def bottle_server(ident, address, settings: Settings, routes: Dict[str, RouteDef]):
    app = Bottle()
    host, port = address

    for x in routes.values():
        app.route(x.route, x.methods, callback=partial(bottle_server_callback, settings=settings, route_def=x),
                  name=x.id)
    app.run(host=host, port=port)


class ServerProcess:
    def __init__(self, ident, address):
        self.ident = ident
        self.address = address
        self.p = None  # type: Optional[subprocess.Popen]

    @property
    def running(self):
        return self.p is not None

    def start(self, settings, routes):
        if self.running:
            raise NotImplementedError('We need to stop first')

        args = [
            sys.executable,
            '-m',
            __name__,
            self.ident,
            argv_encode(self.address),
            argv_encode(settings),
            argv_encode(routes),
        ]

        self.p = subprocess.Popen(args)

    def stop(self):
        if self.running:
            self.p.terminate()
            self.p = None
        else:
            raise NotImplementedError('We need to start first')


if __name__ == '__main__':
    import sys

    bottle_server_process(*sys.argv[1:])
