import json
from typing import Dict, NamedTuple, List, Tuple, Optional
from uuid import uuid4

from etcd3 import Etcd3Client

from tfci.db import ops
from tfci.db.ops import Transaction
from tfci.db.mapper import NamedTupleEx, MapperBase
from tfci_core.const import JOBS_STACK, JOBS_THREAD, JOBS_LOCK

UNSET = object()


class StackFrame(NamedTupleEx, MapperBase):
    id: str
    vals: Dict
    version: int

    @classmethod
    def new(cls, id, vals=None):
        if vals is None:
            vals = {}
        return StackFrame(id, vals, -1)

    def get(self, item):
        return self.vals[item]

    def set(self, key, value):
        self.vals[key] = value

    def serialize(self):
        return json.dumps(self.vals)

    @classmethod
    def deserialize(cls, key, version, bts):
        return StackFrame(key, json.loads(bts), int(version))

    @classmethod
    def key_fn(cls, id):
        return JOBS_STACK % (id,)


class FollowUp(NamedTuple):
    create_threads: List['ThreadContext']
    update_stacks: List[StackFrame]
    create_stacks: List[StackFrame]
    delete_stacks: List[StackFrame]

    @classmethod
    def new(cls, create_threads=None, update_stacks=None, create_stacks=None, delete_stacks=None):
        return FollowUp(
            [] if create_threads is None else create_threads,
            [] if update_stacks is None else update_stacks,
            [] if create_stacks is None else create_stacks,
            [] if delete_stacks is None else delete_stacks,
        )


class ThreadContext(NamedTupleEx, MapperBase):
    id: str
    ip: str  # like: "/sys/eg:entrypoint"

    # we could have instead a list of stack pointers.
    sp: List[str]
    version: int

    @classmethod
    def new(cls, id: str, ip: str, sp=None):
        if sp is None:
            sp = []
        return ThreadContext(id, ip, sp, -1)

    def copy(self):
        return ThreadContext(uuid4().hex, self.ip, self.sp, self.version)

    def serialize(self):
        return json.dumps([self.ip, self.sp])

    @classmethod
    def deserialize(cls, key, version, bts):
        return ThreadContext(key, *json.loads(bts), version)

    @classmethod
    def key_fn(self, id):
        return JOBS_THREAD % (id,)

    @property
    def lock_key(self):
        return JOBS_LOCK % (self.id,)

    def lock(self, db: Etcd3Client, lock_ident, lock_lease) -> Tuple[
        bool, Optional['ThreadContext'], List[Optional[StackFrame]]]:

        tx = Transaction.new().compare(
            ops.Version(self.lock_key) == 0,
            ops.Version(self.key) > 0
        ).success(
            (None, ops.Put(self.lock_key, lock_ident, lease=lock_lease)),
            (ThreadContext, ops.Get(self.key))
        ).failure(
            (ThreadContext, ops.Get(self.key))
        )

        for x in self.sp:
            tx = tx.merge(StackFrame.load(x))

        ok: bool
        item: ThreadContext
        sfs: List[Optional[StackFrame]]
        item_nok: ThreadContext

        ok, (item, *sfs), (item_nok,) = tx.exec(db)

        if not ok:
            return False, item_nok, []
        else:
            if self.sp != item.sp:
                tx_other = self.exists(self.id)

                for x in item.sp:
                    tx_other = tx_other.merge(StackFrame.load(x))

                ok, sfs, _ = tx_other.exec(db)

                assert ok

            return ok, item, sfs

    def update(self, ip: str = UNSET, sp: List[str] = UNSET):
        new_ip = self.ip if ip == UNSET else ip
        new_sp = self.sp if sp == UNSET else sp

        return ThreadContext(self.id, new_ip, new_sp, self.version)

    def follow(
        self,
        db: Etcd3Client,
        lock_ident,
        f: FollowUp
    ):
        updated = None

        if self.id in [x.id for x in f.create_threads]:
            updated = [x for x in f.create_threads if x.id == self.id][0]

        # spinlock-based modification procedure

        # UNLOCK: THREAD

        # todo: we need to realise that the only reason we have locks is to reduce the contention in the queue
        compare = [
            db.transactions.value(self.lock_key) == lock_ident,
        ]

        # CREATE: THREAD
        success = [
            db.transactions.put(y.key, y.serialize()) for y in f.create_threads
        ]

        # UNLOCK

        success += [db.transactions.delete(self.key)] if updated is None else []

        # DELETE LOCK

        success += [
            db.transactions.delete(self.lock_key),
        ]

        # UPDATE: STACK
        compare += [
            db.transactions.version(x.key) == x.version for x in f.update_stacks
        ]

        success += [
            db.transactions.put(x.key, x.serialize()) for x in f.update_stacks
        ]

        # CREATE: STACK
        success += [
            db.transactions.put(x.key, x.serialize()) for x in f.create_stacks
        ]

        # DELETE: STACK
        compare += [
            db.transactions.version(x.key) == x.version for x in f.delete_stacks
        ]

        success += [
            db.transactions.delete(x.key) for x in f.delete_stacks
        ]



        ok, _ = db.transaction(
            compare=compare,
            success=success,
            failure=[]
        )

        return ok, updated

    def unlock(self, db: Etcd3Client, lock_ident):
        ok, _ = db.transaction(
            compare=[
                db.transactions.value(self.lock_key) == lock_ident,
            ],
            success=[
                db.transactions.delete(self.lock_key),
            ],
            failure=[
            ]
        )

        return ok


