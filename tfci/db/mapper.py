import logging
from typing import Type, Optional, Dict, TypeVar, NamedTupleMeta

from etcd3 import Etcd3Client

from tfci.db import ops
from tfci.db.ops import Transaction
from tfci.db.db_util import RangeEvent

T = TypeVar('T')

logger = logging.getLogger(__name__)


class MapperBase:
    @classmethod
    def key_fn(self, id) -> str:
        raise NotImplementedError('')

    @classmethod
    def key_fn_rev(cls, key):
        return key[len(cls.key_fn('')):]

    def serialize(self) -> str:
        raise NotImplementedError('')

    @classmethod
    def deserialize(cls: Type[T], id, version, bts) -> T:
        raise NotImplementedError('')

    @property
    def key(self):
        return self.key_fn(self.id)

    def put(self):
        return Transaction.new().success(
            (None, ops.Put(self.key, self.serialize()))
        )

    def create(self) -> Transaction:
        return Transaction.new().compare(ops.Version(self.key) == 0).merge(self.put())

    def update(self) -> Transaction:
        return Transaction.new().compare(ops.Version(self.key) == self.version).merge(self.put())

    def delete_cmp(self) -> Transaction:
        return Transaction.new().compare(ops.Version(self.key) == self.version).merge(self.delete(self.id))

    @classmethod
    def exists(cls, id) -> Transaction:
        return Transaction.new().compare(ops.Version(cls.key_fn(id)) > 0)

    @classmethod
    def delete(cls, id) -> Transaction:
        return cls.exists(id).success(
            (None, ops.Delete(cls.key_fn(id))),
        )

    @classmethod
    def load(cls, id) -> Transaction:
        return Transaction.new().success(
            (cls, ops.Get(cls.key_fn(id))),
        )

    @classmethod
    def load_exists(cls, id) -> Transaction:
        return cls.exists(id).merge(cls.load(id))

    @classmethod
    def deserialize_watch(cls: Type[T], ev: RangeEvent):
        return cls.deserialize(cls.key_fn_rev(ev.key), ev.version, ev.body)

    @classmethod
    def deserialize_range(cls: Type[T], items) -> Optional[T]:
        if len(items) == 0:
            return None
        elif len(items) > 1:
            assert False, f'you are deserializing a range that is too long? {range}'

        (it, meta), *_ = items

        id = cls.key_fn_rev(meta.key.decode())
        return cls.deserialize(id, meta.version, it)

    @classmethod
    def load_all(cls: Type[T], db: Etcd3Client) -> Dict[str, T]:
        r = {}

        prefix = cls.key_fn('')

        for v, v_m in db.get_prefix(prefix):
            k = cls.key_fn_rev(v_m.key.decode())
            try:
                r[k] = cls.deserialize(k, v_m.version, v)
            except Exception as e:
                raise ValueError(f'While deserializing ID={k}: {e}')

        return r


class NamedTupleMetaEx(NamedTupleMeta):

    def __new__(cls, typename, bases, ns):
        cls_obj = super().__new__(cls, typename, bases, ns)
        if ns.get('_root', False):
            return cls_obj

        bases = bases + (cls_obj,)
        bases = tuple(x for x in bases if x != NamedTupleEx)

        return type(typename, bases, ns)


class NamedTupleEx(metaclass=NamedTupleMetaEx):
    _root = True

    def __new__(self, *args, **kwargs):
        pass
