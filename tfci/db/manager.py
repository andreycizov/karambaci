from argparse import ArgumentParser
from enum import Enum
from typing import Type
from uuid import uuid4

from etcd3 import Etcd3Client

from tfci.db.mapper import MapperBase, logger


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

        cls._arguments_else(args, utility_args)

    @classmethod
    def _arguments_else(cls, args: ArgumentParser, utility_args):
        pass

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
        if utility == EntityAction.List.value:
            hdr = f'----- Listing {self.entity} -------'
            print(hdr)
            for k, v in self.action_ls(**kwargs).items():
                print(k, v)
            print('-' * len(hdr))
        elif utility == EntityAction.Add.value:
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
        elif utility == EntityAction.Remove.value:
            check, succ = self.action_rm(**kwargs)

            ok, _ = self.db.transaction(compare=check, success=succ, failure=[])

            if ok:
                print('OK')
            else:
                print('Failure')
        else:
            self._action_else(utility, **kwargs)

    def action_ls(self, **kwargs):
        return self.model.load_all(self.db)

    def action_add(self, **kwargs):
        raise NotImplementedError('')

    def action_rm(self, id, **kwargs):
        a, b = self.model.delete(self.db, id)
        return [a], [b]

    def _action_else(self, utility, **kwargs):
        logger.warning(f'Unknown action: {utility}')