import json
from typing import Dict, NamedTuple, Optional, Tuple, List
from uuid import uuid4

from etcd3 import Etcd3Client

from tfci_core.const import JOBS_THREAD, JOBS_LOCK, JOBS_STACK

UNSET = object()


class StackFrame(NamedTuple):
    id: str
    version: int
    vals: Dict

    @classmethod
    def new(cls, vals=None):
        if vals is None:
            vals = {}
        return StackFrame(uuid4().hex, -1, vals)

    def get(self, item):
        return self.vals[item]

    def set(self, key, value):
        self.vals[key] = value

    def serialize(self):
        return json.dumps(self.vals)

    @classmethod
    def deserialize(cls, key, version, bts):
        return StackFrame(key, int(version), json.loads(bts))

    @classmethod
    def load(cls, db: Etcd3Client, id):
        x, kv_meta = db.get(StackFrame.stack_key(id))

        if x is None:
            return None

        return StackFrame.deserialize(id, kv_meta.version, x)

    @classmethod
    def stack_key(cls, id):
        return JOBS_STACK % (id,)

    @property
    def key(self):
        return StackFrame.stack_key(self.id)


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


class ThreadContext(NamedTuple):
    id: str
    ip: str  # like: "/sys/eg:entrypoint"

    # we could have instead a list of stack pointers.
    sp: List[str]

    @classmethod
    def new(cls, ip, sp=None):
        if sp is None:
            sp = []
        return ThreadContext(uuid4().hex, ip, sp)

    def copy(self):
        return ThreadContext(uuid4().hex, self.ip, self.sp)

    def serialize(self):
        return json.dumps([self.ip, self.sp])

    @classmethod
    def deserialize(cls, key, bts):
        return ThreadContext(key, *json.loads(bts))

    @property
    def key(self):
        r = (JOBS_THREAD % (self.id,)).encode()
        return r

    @property
    def lock_key(self):
        return JOBS_LOCK % (self.id,)

    def lock(self, db: Etcd3Client, lock_ident, lock_lease) -> Tuple[
        bool, Optional['ThreadContext'], List[Optional[StackFrame]]]:

        ok, vals = db.transaction(
            compare=[
                db.transactions.version(self.lock_key) == 0,
                db.transactions.version(self.key) > 0,
            ],
            success=[
                        db.transactions.put(self.lock_key, lock_ident, lease=lock_lease),
                        db.transactions.get(self.key),
                    ] + [db.transactions.get(StackFrame.stack_key(s)) for s in
                         (self.sp if self.sp is not None else [])],
            failure=[
                db.transactions.get(self.key)
            ]
        )

        def parse_stacks(stacks):
            ret_stacks = []

            for x in stacks:
                if len(x) == 0:
                    ret_stacks.append(None)
                else:
                    (stack_item, stack_meta), *_ = x
                    ret_stack = StackFrame.deserialize(stack_meta.key.decode()[len(JOBS_STACK % ('',)):],
                                                       stack_meta.version, stack_item)

                    ret_stacks.append(ret_stack)

            return ret_stacks

        if not ok:
            return False, None, []
        else:

            ret_stacks = []

            _, threads, *stacks = vals

            if not len(threads):
                thread = None
            else:
                (thread_item, thread_meta), *_ = threads
                thread = ThreadContext.deserialize(thread_meta.key.decode()[len(JOBS_THREAD % ('',)):], thread_item)

            if self.sp == thread.sp:
                ret_stacks = parse_stacks(stacks)
            else:
                _, items = db.transaction(
                    compare=[
                        db.transactions.version('29384092rjufiosdajfoasdjfpdiosfjs') == 0
                    ],
                    success=[
                        db.transactions.get(StackFrame.stack_key(s)) for s in
                        (thread.sp if thread.sp is not None else [])
                    ],
                    failure=[

                    ]
                )

                ret_stacks = parse_stacks(items)

            return ok, thread, ret_stacks

    def update(self, ip: str = UNSET, sp: List[str] = UNSET):
        new_ip = self.ip if ip == UNSET else ip
        new_sp = self.sp if sp == UNSET else sp

        return ThreadContext(self.id, new_ip, new_sp)

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

        success = [db.transactions.delete(self.key)] if updated is None else []

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

        # CREATE: THREAD
        success += [
            db.transactions.put(y.key, y.serialize()) for y in f.create_threads
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
