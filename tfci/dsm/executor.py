from typing import NamedTuple, Optional, List

from etcd3 import Etcd3Client

from tfci.dsl.ast import Identifier, Constant
from tfci.dsl.struct import OpcodeArgs
from tfci.settings import Settings
from tfci_core.daemons.worker.struct import ThreadContext, StackFrame


class ExecutionError(Exception):
    pass


class ExecutionSingleton:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._db = None  # type: Optional[Etcd3Client]

    @property
    def db(self) -> Etcd3Client:
        if self._db is None:
            self._db = self.settings.get_db()
        return self._db


class ExecutionContext(NamedTuple):
    singleton: ExecutionSingleton
    args: OpcodeArgs
    nip: Optional[str]
    thread: ThreadContext
    stack: List[StackFrame]
    stacks_updated: set = set()

    def resolve_item(self, arg):
        if isinstance(arg, Identifier):
            return self.stack_get(arg.name, arg.level)
        elif isinstance(arg, Constant):
            return arg.value
        else:
            raise NotImplementedError(f'{arg}')

    @classmethod
    def stack_idx(cls, level):
        return level

    def stack_load(self, level=0):
        r = self.stack[self.stack_idx(level)]

        if r is None:
            raise ExecutionError(
                f'(push) `{self.thread.id}:{self.thread.ip}` stack at level=`{level}` is NULL') from None
        return r

    def stack_get(self, name, level=0):
        try:
            return self.stack_load(level).get(name)
        except IndexError:
            raise ExecutionError(
                f'(push) `{self.thread.id}:{self.thread.ip}` references `{name}:{level}` but Levels=`{len(self.stack)}`') from None
        except KeyError:
            raise ExecutionError(
                f'(push) `{self.thread.id}:{self.thread.ip}` references `{name}:{level}` but name does not exist') from None

    def stack_set(self, val, name, level=0):
        try:
            self.stack_load(level).set(name, val)
            self.stacks_updated.add(self.stack_idx(level))
        except IndexError:
            raise ExecutionError(
                f'(push) `{self.thread.id}:{self.thread.ip}` references `{name}:{level}` but Levels=`{len(self.stack)}`')
        except KeyError:
            raise ExecutionError(
                f'(push) `{self.thread.id}:{self.thread.ip}` references `{name}:{level}` but name does not exist')

    def resolve_arg(self, idx):
        if len(self.args) <= idx:
            raise ExecutionError(f'`{self.thread.id}:{self.thread.ip}` cannot resolve arg at index {idx}: too few args')

        jmp = self.args[idx]

        if isinstance(jmp, Identifier):
            jmp = self.stack_get(jmp.name, jmp.level)
        elif isinstance(jmp, Constant):
            jmp = jmp.value
        else:
            raise ExecutionError(f'(j) `{self.thread.id}:{self.thread.ip}` Incorrect jmp address: {repr(jmp)}')

        return jmp