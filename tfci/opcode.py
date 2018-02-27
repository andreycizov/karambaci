import inspect
from pprint import pprint
from typing import Union, Callable, List, Generic, TypeVar, Type

from tfci.dsl.ast import Constant, Identifier, Map, Command
from tfci.dsl.exception import CompilerException
from tfci.dsl.struct import OpcodeArgs
from tfci.dsm.executor import ExecutionContext
from tfci.dsm.struct import FollowUp

T = TypeVar('T')


class OpArg(Generic[T]):
    def __init__(self, ctx: ExecutionContext, arg: Union[Constant, Identifier]):
        self.ctx = ctx
        self.arg = arg

    def get(self) -> T:
        if isinstance(self.arg, Identifier):
            return self.ctx.stack_get(
                self.arg.name,
                self.arg.level
            )
        elif isinstance(self.arg, Constant):
            return self.arg.value
        else:
            raise NotImplementedError(f'{self.arg}')

    @classmethod
    def map(cls, ctx, arg):
        if isinstance(arg, Identifier):
            return RefOpArg(ctx, arg)
        elif isinstance(arg, Constant):
            return OpArg(ctx, arg)
        else:
            raise NotImplementedError(f'{ctx} {arg}')


class RefOpArg(OpArg, Generic[T]):
    def set(self, value: T):
        assert not isinstance(self.arg, Constant), 'Cannot set a constant argument'

        self.ctx.stack_set(
            value,
            self.arg.name,
            self.arg.level
        )


OpcodeFunction = Callable[[ExecutionContext], FollowUp]


class opcode:
    def __init__(self, name, is_simple=True):
        assert not callable(name), 'Correct usage: @opcode(opcode_name), not @opcode'
        self.name = name
        self.is_simple = is_simple

    def __call__(self, fn: OpcodeFunction):
        if self.is_simple:
            return type(self.name, (OpcodeDef,), {'fn': fn, 'name': self.name})
        else:
            return type(self.name, (SysOpcodeDef,), {'fn': fn, 'name': self.name})


class OpcodeDef:
    name = None  # type: str

    def __init__(self):
        assert self.name is not None, 'name attribute must be set'

    def fn(self, *args, **kwargs):
        raise NotImplementedError('')

    def check(self, c: Command):
        pass

    @classmethod
    def find_module(cls, mod) -> List[Type['OpcodeDef']]:
        return [x for x in (getattr(mod, x) for x in dir(mod)) if inspect.isclass(x) and issubclass(x, OpcodeDef)]

    def __call__(self, ctx: ExecutionContext) -> FollowUp:
        def map_args(args: OpcodeArgs):
            pos = []
            kw = {}
            for i, arg in enumerate(args):
                if isinstance(arg, Identifier) or isinstance(arg, Constant):
                    pos.append(OpArg.map(ctx, arg))
                elif isinstance(arg, Map):
                    kw[arg.identifier.name] = OpArg.map(ctx, arg.to)
                else:
                    raise NotImplementedError(f'{args}')
            return pos, kw

        args, kwargs = map_args(ctx.args)

        self.fn(ctx, *args, **kwargs)

        return FollowUp.new(
            create_threads=[ctx.thread.update(ip=ctx.nip)],
            update_stacks=[ctx.stack[x] for x in ctx.stacks_updated]
        )


class SysOpcodeDef(OpcodeDef):
    def __init__(self):
        pass

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        raise NotImplementedError('')

    def check(self, c: Command):
        pass

    def __call__(self, ctx: ExecutionContext) -> FollowUp:
        return self.fn(ctx)
