import json
import logging
from json import JSONDecodeError
from typing import NamedTuple, Dict, Any, List, Optional, TypeVar, Type
from uuid import uuid4

from etcd3 import Etcd3Client

from tfci_core.daemons.worker.struct import ThreadContext, StackFrame
from tfci_docker.struct import ORMMeta
from tfci_http.const import http_req_key, http_rep_key, http_sub_key

logger = logging.getLogger(__name__)


class RouteDef(NamedTuple, ORMMeta):
    id: str
    methods: List[str]
    route: str
    ep: str
    stack: Dict[str, Any]
    version: int

    @classmethod
    def new(cls, id, methods, route, ep, stack):
        return RouteDef(id, methods, route, ep, stack, -1)

    @classmethod
    def key_fn(cls, id):
        return http_sub_key(id)

    @property
    def key(self):
        return http_sub_key(self.id)

    def create(self, db: Etcd3Client):
        return db.transactions.version(self.key) == 0, db.transactions.put(self.key, self.serialize())

    def update(self, db: Etcd3Client):
        return db.transactions.version(self.key) == self.version, db.transactions.put(self.key, self.serialize())

    def serialize(self):
        return json.dumps([self.methods, self.route, self.ep, self.stack])


    @classmethod
    def deserialize(cls, id, version, bts):
        return RouteDef(id, *json.loads(bts), version)


class Request(NamedTuple):
    id: str

    @classmethod
    def new(cls):
        return Request(uuid4().hex)

    @property
    def key(self):
        return http_req_key(self.id)

    def serialize(self):
        return b''

    @classmethod
    def deserialize(cls, key, bts):
        return Request(key)

    @classmethod
    def initiate(cls, db: Etcd3Client, route_def: RouteDef, r: 'Request', *args, **kwargs):
        sf = StackFrame.new(
            {
                **kwargs,
                'req_id': r.id
            }
        )

        ctx = ThreadContext.new(route_def.ep, [sf.id])

        ok, _ = db.transaction(
            compare=[
                db.transactions.version(r.key) == 0
            ],
            success=[
                db.transactions.put(r.key, r.serialize()),
                db.transactions.put(sf.key, sf.serialize()),
                db.transactions.put(ctx.key, ctx.serialize()),
            ], failure=[

            ]
        )

        return ok, r


class Reply(NamedTuple):
    id: str
    result: Optional[Dict[str, Any]]

    @property
    def key(self):
        return Reply.key_fn(self.id)

    @classmethod
    def key_fn(cls, id):
        return http_rep_key(id)

    def serialize(self):
        return json.dumps([self.result])

    @classmethod
    def deserialize(cls, key, bts):
        return Reply(key, *json.loads(bts))
