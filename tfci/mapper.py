from argparse import ArgumentParser
from enum import Enum
from typing import Type, Optional, Dict, TypeVar, NamedTupleMeta
from uuid import uuid4

from etcd3 import Etcd3Client

T = TypeVar('T')


class MapperBase:
    @classmethod
    def key_fn(self, id) -> str:
        raise NotImplementedError('')

    def serialize(self) -> str:
        raise NotImplementedError('')

    @classmethod
    def deserialize(cls: Type[T], id, version, bts) -> T:
        raise NotImplementedError('')

    @property
    def key(self):
        return self.key_fn(self.id)

    def put(self, db: Etcd3Client):
        return db.transactions.put(self.key, self.serialize())

    def create(self, db: Etcd3Client):
        return db.transactions.version(self.key) == 0, self.put(db)

    def update(self, db: Etcd3Client):
        return db.transactions.version(self.key) == self.version, self.put(db)

    def delete_cmp(self, db: Etcd3Client, id):
        return db.transactions.version(self.key) == self.version, db.transactions.delete(self.key)

    @classmethod
    def exists(cls, db: Etcd3Client, id):
        return db.transactions.version(cls.key_fn(id)) > 0

    @classmethod
    def delete(cls, db: Etcd3Client, id):
        return cls.exists(db, id), db.transactions.delete(cls.key_fn(id))

    @classmethod
    def load(cls, db: Etcd3Client, id):
        return cls.exists(db, id), db.transactions.get(cls.key_fn(id))

    @classmethod
    def deserialize_range(cls: Type[T], items) -> Optional[T]:
        if items is None:
            return None
        elif len(items) == 0:
            return None
        elif len(items) > 1:
            assert False, f'you are deserializing a range that is too long? {range}'

        (it, meta), *_ = items

        id = meta.key.decode()[len(cls.key_fn('')):]
        return cls.deserialize(id, meta.version, it)

    @classmethod
    def load_all(cls: Type[T], db: Etcd3Client) -> Dict[str, T]:
        r = {}

        prefix = cls.key_fn('')

        for v, v_m in db.get_prefix(prefix):
            k = v_m.key.decode()[len(prefix):]
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


class EntityAction(Enum):
    List = 'ls'
    Add = 'add'
    Remove = 'rm'


class EntityManager:
    entity = '<example>'
    model: Type[MapperBase]

    def __init__(self, db: Etcd3Client):
        self.db = db

    @classmethod
    def arguments(cls, args: ArgumentParser):
        utility_args = args.add_subparsers(
            title="utility",
            dest="utility",
            help="select the utility to run",
            metavar="UTIL",
        )
        utility_args.required = True

        util_ls = utility_args.add_parser(EntityAction.List.value, help=f'list created {cls.entity}')
        cls.arguments_ls(util_ls)
        util_add = utility_args.add_parser(EntityAction.Add.value, help=f'add {cls.entity}')
        cls.arguments_add(util_add)
        util_rm = utility_args.add_parser(EntityAction.Remove.value, help=f'remove {cls.entity}')
        cls.arguments_rm(util_rm)

    @classmethod
    def arguments_ls(cls, args: ArgumentParser):
        pass

    @classmethod
    def arguments_add(cls, args: ArgumentParser):
        args.add_argument(
            '-I',
            dest='id',
            required=False,
            default=None,
        )

    @classmethod
    def arguments_rm(cls, args: ArgumentParser):
        args.add_argument(
            dest='id',
        )

    def action(self, utility, **kwargs):
        utility = EntityAction(utility)
        if utility == EntityAction.List:
            hdr = f'----- Listing {self.entity} -------'
            print(hdr)
            for k, v in self.action_ls(**kwargs).items():
                print(k, v)
            print('-' * len(hdr))
        elif utility == EntityAction.Add:
            id = kwargs['id']

            if id is None:
                id = uuid4().hex
                print(f'Generated ID {id}')

            kwargs['id'] = id

            check, succ = self.action_add(**kwargs)

            ok, _ = self.db.transaction(compare=check, success=succ, failure=[])

            if ok:
                print('OK')
            else:
                print('Failure')
        elif utility == EntityAction.Remove:
            check, succ = self.action_rm(**kwargs)

            ok, _ = self.db.transaction(compare=check, success=succ, failure=[])

            if ok:
                print('OK')
            else:
                print('Failure')
        else:
            raise NotImplementedError(str(utility))

    def action_ls(self, **kwargs):
        return self.model.load_all(self.db)

    def action_add(self, **kwargs):
        raise NotImplementedError('')

    def action_rm(self, id, **kwargs):
        a, b = self.model.delete(self.db, id)
        return [a], [b]
