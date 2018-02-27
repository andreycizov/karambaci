import json
from typing import List

from etcd3 import Etcd3Client

from tfci.db.mapper import MapperBase, NamedTupleEx
from tfci.dsm.struct import StackFrame, ThreadContext


class FrozenThreadContext(NamedTupleEx, MapperBase):
    id: str
    ctx: ThreadContext
    version: int

    @classmethod
    def new(cls, id, ep, sp: List[str]):
        return FrozenThreadContext(id, ThreadContext(id, ep, sp, -1), -1)

    @classmethod
    def key_fn(cls, id):
        return f'/jobs/frozen/{id}'

    @classmethod
    def get(cls, db: Etcd3Client, id):
        return db.transactions.get(cls.key_fn(id))

    def serialize(self):
        return json.dumps([self.ctx.id, self.ctx.ip, self.ctx.sp])

    @classmethod
    def deserialize(cls, key, version, bts):
        id, *its = json.loads(bts)
        return FrozenThreadContext(id, ThreadContext(id, *its, version), version)

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

    def call(self, db: Etcd3Client, sf: StackFrame):
        ctx = self.ctx.copy()

        ctx = ctx.update(sp=[sf.id] + ctx.sp)

        return ctx.create(db)
