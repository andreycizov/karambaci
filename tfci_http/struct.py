import json
import logging
from typing import Dict, Any, List, Optional
from uuid import uuid4

from etcd3 import Etcd3Client

from tfci_core.daemons.worker.struct import StackFrame
from tfci.db.mapper import MapperBase, NamedTupleEx
from tfci_std.struct import FrozenThreadContext

logger = logging.getLogger(__name__)


class RouteDef(NamedTupleEx, MapperBase):
    id: str
    methods: List[str]
    route: str
    frz_id: str
    version: int

    @classmethod
    def new(cls, id, methods, route, frz_id):
        return RouteDef(id, methods, route, frz_id, -1)

    @classmethod
    def key_fn(cls, id):
        return '/http/subs/%s' % (id,)

    def serialize(self):
        return json.dumps([self.methods, self.route, self.frz_id])

    @classmethod
    def deserialize(cls, id, version, bts):
        return RouteDef(id, *json.loads(bts), version)


class Request(NamedTupleEx, MapperBase):
    id: str
    version: int

    @classmethod
    def new(cls):
        return Request(uuid4().hex, -1)

    @classmethod
    def key_fn(self, id):
        return '/http/requests/%s' % (id,)

    def serialize(self):
        return b''

    @classmethod
    def deserialize(cls, key, version, bts):
        return Request(key, version)

    @classmethod
    def initiate(cls, db: Etcd3Client, frz: FrozenThreadContext, r: 'Request', stack):
        # we need:
        # - create a new stack frame in the context of the http request

        sf = StackFrame.new(
            uuid4().hex,
            {
                **stack,
                'req_id': r.id
            }
        )

        a1, b1 = frz.call(db, sf)
        a2, b2 = sf.create(db)

        ok, _ = db.transaction(
            compare=[
                a1,
                a2
            ],
            success=[
                b1,
                b2,
            ], failure=[

            ]
        )

        return ok, r


class Reply(NamedTupleEx, MapperBase):
    id: str
    result: Optional[Dict[str, Any]]
    version: int

    @classmethod
    def new(cls, id, body):
        return Reply(id, body, -1)

    @classmethod
    def key_fn(cls, id):
        return '/http/replies/%s' % (id,)

    def serialize(self):
        return json.dumps([self.result])

    @classmethod
    def deserialize(cls, key, version, bts):
        return Reply(key, *json.loads(bts), version)
