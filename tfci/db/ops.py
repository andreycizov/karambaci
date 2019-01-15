from typing import NamedTuple, Tuple, Optional, Type

from etcd3 import Etcd3Client
from etcd3.transactions import *

from tfci.db.db_util import TX_MOD

MapTupleItem = Optional[Type['mapper.MapperBase']]
MapTuple = Tuple[MapTupleItem, ...]

Compare = BaseCompare
Modify = TX_MOD


class Transaction(NamedTuple):
    comp: Tuple[Compare, ...]
    succ: Tuple[Modify, ...]
    fail: Tuple[Modify, ...]

    succ_map: MapTuple
    fail_map: MapTuple

    # def new(cls, compare=None, success=None, failure=None):
    @classmethod
    def new(
        cls,
        compare: Tuple[Compare, ...] = tuple(),
        success: Tuple[Modify, ...] = tuple(),
        failure: Tuple[Modify, ...] = tuple(),
        success_map: Optional[MapTuple] = None,
        failure_map: Optional[MapTuple] = None,
    ):

        if success_map is None:
            success_map = (None,) * len(success)

        if failure_map is None:
            failure_map = (None,) * len(failure)

        assert len(success) == len(success_map)
        assert len(failure) == len(failure_map)

        return Transaction(compare, success, failure, success_map, failure_map)

    def compare(self, *compare: Compare):
        return self.merge(Transaction.new(compare=compare))

    def success(self, *ops: Tuple[Optional[MapTuple], Modify]):
        ops_map = tuple(x for x, y in ops)
        ops = tuple(y for x, y in ops)

        return self.merge(Transaction.new(success=ops, success_map=ops_map))

    def failure(self, *ops: Tuple[Optional[MapTuple], Modify]):
        ops_map = tuple(x for x, y in ops)
        ops = tuple(y for x, y in ops)

        return self.merge(Transaction.new(failure=ops, failure_map=ops_map))

    def merge(self, o: 'Transaction'):
        return Transaction.new(
            self.comp + o.comp,
            self.succ + o.succ,
            self.fail + o.fail,
            self.succ_map + o.succ_map,
            self.fail_map + o.fail_map,
        )

    def __and__(self, o: 'Transaction'):
        return self.merge(o)

    def exec(self, db: Etcd3Client):

        ok, items = db.transaction(
            compare=self.comp,
            success=self.succ,
            failure=self.fail,
        )

        # we might want the mapper not to be so strict.

        if ok:
            items_ok = [y.deserialize_range(z) for z, y in zip(items, self.succ_map) if y]
        else:
            items_ok = [None for _ in self.succ_map]

        if not ok:
            items_fail = [y.deserialize_range(z) for z, y in zip(items, self.fail_map) if y]
        else:
            items_fail = [None for _ in self.fail_map]

        return ok, items_ok, items_fail
