# given a definition of mappings between the paths
# serve requests
import sys
import setproctitle
import subprocess

from functools import partial
from typing import Optional, Dict
from uuid import uuid4

from bottle import Bottle
from bottle import response
from etcd3 import Etcd3Client

from tfci.settings import Settings
from tfci.time import time_now
from tfci_core.daemons.db_util import watch_range_single, Ev
from tfci_core.daemons.generic.pool import argv_decode, argv_encode
from tfci.dsm.struct import StackFrame
from tfci_http.struct import RouteDef, Request, Reply
from tfci_std.struct import FrozenThreadContext


def bottle_server_process(ident, address, settings, routes):
    setproctitle.setproctitle(f'http-{ident}')

    address = argv_decode(address)
    settings = argv_decode(settings)  # type: Settings
    routes = argv_decode(routes)  # type: Dict[str, RouteDef]

    bottle_server(ident, address, settings, routes)


class Singleton:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._db = None

    @property
    def db(self) -> Etcd3Client:
        if not self._db:
            self._db = self.settings.get_db()
        return self._db


def bottle_server_callback(singleton: Singleton, route_def: RouteDef, frz: FrozenThreadContext, *args, **kwargs):
    db = singleton.db

    stack = {**kwargs}

    time_start = time_now()
    ok = False
    body = {}

    try:
        r = Request.new()

        token = watch_range_single(db, Reply.key_fn(r.id))

        ok, r = Request.initiate(db, frz, r, stack)

        if ok:
            ev = token.get(timeout=180, close=False)
            assert ev.event == Ev.Put, f'{r.key} {ev}'

            r = Reply.deserialize(r.key, ev.version, ev.body)

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

    singleton = Singleton(settings)

    for x in routes.values():
        cmp, succ = FrozenThreadContext.load(singleton.db, x.frz_id)

        ok, (items,) = singleton.db.transaction(compare=[cmp], success=[succ], failure=[])

        assert ok

        frz = FrozenThreadContext.deserialize_range(items)

        app.route(x.route, x.methods, callback=partial(bottle_server_callback, singleton=singleton, route_def=x,
                                                       frz=frz),
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
