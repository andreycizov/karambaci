from uuid import uuid4

from tfci.dsl.ast import Identifier, Constant, Map, Command
from tfci.dsl.exception import CompilerException
from tfci.dsl.struct import OpcodeArgs
from tfci.dsm.executor import ExecutionError, ExecutionContext
from tfci.opcode import opcode, SysOpcodeDef
from tfci_core.daemons.worker.struct import FollowUp, StackFrame, ThreadContext


class NopOpcode(SysOpcodeDef):
    name = 'nop'

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        if ctx.nip:
            a = [ctx.thread.update(ip=ctx.nip)]
        else:
            a = []

        return FollowUp.new(a)


def push_pop(csf: StackFrame, ctx: ExecutionContext):
    for arg in ctx.args:
        if isinstance(arg, Identifier):
            csf.set(arg.name, ctx.stack_get(arg.name, arg.level))
        elif isinstance(arg, Constant):
            raise ExecutionError(f'(push) `{ctx.thread.id}:{ctx.thread.ip}` constant push requires an identifier')
        elif isinstance(arg, Map):
            if isinstance(arg.to, Constant):
                val = arg.to.value
            elif isinstance(arg.to, Identifier):
                val = ctx.stack_get(arg.to.name, arg.to.level)
            else:
                raise NotImplementedError(f'{repr(arg)}')

            csf.set(arg.identifier.name, val)
        else:
            raise NotImplementedError(f'{repr(arg)}')


class PushOpcode(SysOpcodeDef):
    name = 'push'

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        # how do we know which stacks have been changed ?

        csf = StackFrame.new()

        push_pop(csf, ctx)

        return FollowUp.new(create_threads=[ctx.thread.update(ip=ctx.nip, sp=[csf.id] + ctx.thread.sp)],
                            create_stacks=[csf])


class PopOpcode(SysOpcodeDef):
    name = 'pop'

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        # so a stack frame needs to load the parent stack frame
        # then update it

        # rtn = ctx.stack_get('psp')
        #
        # x = StackFrame.load(ctx.singleton.db, rtn)

        if len(ctx.stack) == 0:
            raise ExecutionError(f'`{ctx.thread.id}:{ctx.thread.ip}` Stack underflow')

        if len(ctx.args) and len(ctx.stack) < 2:
            raise ExecutionError(f'`{ctx.thread.id}:{ctx.thread.ip}` Stack underflow')
        elif len(ctx.args):
            push_pop(ctx.stack[-2], ctx)

        return FollowUp.new([ctx.thread.update(ip=ctx.nip, sp=ctx.thread.sp[1:])], update_stacks=[ctx.stack[1]])


class ClrOpcode(SysOpcodeDef):
    name = 'clr'

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        # if we had access to the meta-info, then we could easily remove it ourselves

        if len(ctx.stack) == 0:
            raise ExecutionError(f'`{ctx.thread.id}:{ctx.thread.ip}` Stack underflow')

        if len(ctx.args) and len(ctx.stack) < 2:
            raise ExecutionError(f'`{ctx.thread.id}:{ctx.thread.ip}` Stack underflow')
        elif len(ctx.args):
            push_pop(ctx.stack[-2], ctx)

        upd = []

        if len(ctx.stack) > 1:
            upd = [ctx.stack[1]]

        return FollowUp.new([ctx.thread.update(ip=ctx.nip, sp=ctx.thread.sp[1:])], update_stacks=upd,
                            delete_stacks=[ctx.stack[0]])


class JOpcode(SysOpcodeDef):
    name = 'j'

    def check(self, c: Command):
        if len(c.args) != 1:
            raise CompilerException(c.loc, f"{self.name} needs 1 argument")

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        if len(ctx.args) != 1:
            raise ExecutionError(f'(j) `{ctx.thread.id}:{ctx.thread.ip}` too many args: {ctx.args}')

        jmp = ctx.resolve_arg(0)

        return FollowUp.new([ctx.thread.update(ip=jmp)])


class JEOpcode(SysOpcodeDef):
    name = 'je'

    def check(self, c: Command):
        if len(c.args) != 3:
            raise CompilerException(c.loc, f"{self.name} needs 3 arguments")

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        a, b, jmp_if_true = ctx.resolve_arg(0), ctx.resolve_arg(1), ctx.resolve_arg(2)

        nip = ctx.nip
        if a == b:
            nip = jmp_if_true

        return FollowUp.new([ctx.thread.update(ip=nip)])


class JNEOpcode(SysOpcodeDef):
    name = 'jne'

    def check(self, c: Command):
        if len(c.args) != 3:
            raise CompilerException(c.loc, f"{self.name} needs 3 arguments")

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        a, b, jmp_if_true = ctx.resolve_arg(0), ctx.resolve_arg(1), ctx.resolve_arg(2)

        nip = ctx.nip
        if a != b:
            nip = jmp_if_true

        return FollowUp.new([ctx.thread.update(ip=nip)])


class LdOpcode(SysOpcodeDef):
    name = 'ld'

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        if len(ctx.stack) == 0:
            raise ExecutionError(f'`{ctx.thread.id}:{ctx.thread.ip}` Stack underflow')

        push_pop(ctx.stack[0], ctx)

        return FollowUp.new([ctx.thread.update(ip=ctx.nip)], update_stacks=[ctx.stack[0]])


class HLTOpcode(SysOpcodeDef):
    name = 'hlt'

    def check(self, c: Command):
        if len(c.args) != 0:
            raise CompilerException(c.loc, f"{self.name} does not accept arguments")

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        return FollowUp.new()


class ForkOpcode(SysOpcodeDef):
    name = 'fork'

    def fn(self, ctx: ExecutionContext) -> FollowUp:
        if len(ctx.args) != 1:
            raise ExecutionError(f'(fork) `{ctx.thread.id}:{ctx.thread.ip}` too many args: {ctx.args}')

        jmp = ctx.resolve_arg(0)

        return FollowUp.new([
            ctx.thread.update(ip=ctx.nip),
            ThreadContext.new(jmp, ctx.thread.sp),
        ])
