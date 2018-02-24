import json
from typing import NamedTuple

from etcd3 import Etcd3Client

from tfci_core.daemons.worker.struct import ThreadContext


class FrozenThreadContext(NamedTuple):
    id: str
    ctx: ThreadContext
    version: int

    @classmethod
    def key_fn(cls, id):
        return f'/jobs/frozen/{id}'.encode()

    @classmethod
    def get(cls, db: Etcd3Client, id):
        return db.transactions.get(cls.key_fn(id))

    @property
    def key(self):
        return self.key_fn(self.id)

    def serialize(self):
        return json.dumps([self.ctx.id, self.ctx.ip, self.ctx.sp])

    def unfreeze(self, db: Etcd3Client):
        ok, _ = db.transaction(
            compare=[
                db.transactions.version(self.ctx.key) == 0,
                db.transactions.version(self.key) == self.version,
            ], success=[
                db.transactions.put(self.ctx.key, self.ctx.serialize()),
                db.transactions.delete(self.key),
            ]
        )

        return ok

    @classmethod
    def deserialize(cls, key, version, bts):
        return FrozenThreadContext(key, ThreadContext(*json.loads(bts)), version)