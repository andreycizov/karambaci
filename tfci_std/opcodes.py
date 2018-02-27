import logging
import uuid
from typing import List, Any

from tfci.dsl.ast import Command
from tfci.dsl.exception import CompilerException
from tfci.opcode import OpcodeDef, OpArg, RefOpArg, SysOpcodeDef
from tfci.dsm.executor import ExecutionError, ExecutionContext
from tfci.dsm.struct import FollowUp
from tfci_std.struct import FrozenThreadContext


class WrappedArgs:
    def __init__(self, ctx: ExecutionContext, args: List[OpArg[Any]]):
        self.ctx = ctx
        self.args = args

    def __getitem__(self, key):
        if isinstance(key, slice):
            return [x.get() for x in self.args.__getitem__(key)]
        else:
            return self.args[key].get()


class ExecOpcode(OpcodeDef):
    name = 'exr'

    def fn(self, ctx: ExecutionContext, code: OpArg[str], ret: RefOpArg[Any], *args: OpArg[Any]):
        item = compile(code.get(), '<string>', mode='eval')

        try:
            res = eval(
                item,
                {'x': WrappedArgs(ctx, [ret] + list(args))}
            )
        except Exception as e:
            raise ExecutionError("Failed to exr") from e

        ret.set(res)


class LoggerOpcode(OpcodeDef):
    name = "logger"

    def fn(self, ctx: ExecutionContext, level: OpArg[str], format: OpArg[str], *args: OpArg[Any], **kwargs: OpArg[Any]):
        logger = logging.getLogger('OPCODE')

        try:
            level_str = level.get().upper()
            format_str = format.get()
            level_int = logging._nameToLevel[level_str]
        except KeyError:
            raise ExecutionError(f'Incorrect level: {level_str}')
        else:
            logger.log(
                level_int,
                format_str.format(
                    *[x.get() for x in args],
                    **{k: v.get() for k, v in kwargs.items()}
                )
            )


class UUID4Opcode(OpcodeDef):
    name = 'uuid4'

    def fn(self, ctx: ExecutionContext, dest: RefOpArg[str]):
        dest.set(uuid.uuid4().hex)


class FreezeOpcode(OpcodeDef):
    name = 'frz'

    def check(self, c: Command):
        if len(c.args) != 2:
            raise CompilerException(
                c.loc,
                "`frz` requires 2 arguments"
            )

    def fn(self, ctx: ExecutionContext, dest: OpArg[str], cb: OpArg[str]):
        copy_thread = ctx.thread.copy().update(ip=cb.get())
        frz = FrozenThreadContext(dest.get(), copy_thread, -1)

        db = ctx.singleton.db

        cmp, succ = frz.create(db)

        ok, _ = ctx.singleton.db.transaction(
            compare=[
                cmp
            ], success=[
                succ
            ]
        )


class FreezeForkOpcode(SysOpcodeDef):
    name = 'frz_fork'

    def check(self, c: Command):
        if len(c.args) != 1:
            raise CompilerException(
                c.loc,
                "`frz_fork` requires 1 arguments"
            )

    def fn(self, ctx: ExecutionContext):
        frz_id = ctx.resolve_arg(0)
        cmp, succ = FrozenThreadContext.load(ctx.singleton.db, frz_id)

        ok, items = ctx.singleton.db.transaction(
            compare=[
                cmp
            ], success=[
                succ
            ]
        )

        if not ok:
            raise ExecutionError(f'Could not find `{frz_id}`')

        frz = FrozenThreadContext.deserialize_range(items)

        ctx.thread.update(ip=frz.ctx.ip, sp=ctx.thread.sp + frz.ctx.sp)

        return FollowUp.new([
            ctx
        ])



class LockCreateOpcode(OpcodeDef):
    name = 'lock_create'

    def fn(self, ctx: ExecutionContext, lock_id: OpArg[str]):
        ctx.singleton.db.put(f'/locks/{lock_id.get()}', '')


class LockTryLockOpcode(OpcodeDef):
    name = 'lock_try_lock'

    def fn(self, ctx: ExecutionContext, lock_id: OpArg[str], locking_id: OpArg[str], is_locked: RefOpArg[bool]):
        db = ctx.singleton.db

        locking_id = locking_id.get()

        kn = f'/locks/{lock_id.get()}'

        ok, resp = db.transaction(
            compare=[
                db.transactions.value(kn) == ''
            ],
            success=[
                db.transactions.put(kn, locking_id)
            ],
            failure=[
                db.transactions.get(kn)
            ]
        )

        if ok:
            is_locked.set(True)
        elif not len(resp):
            is_locked.set(False)
        else:
            # not locked now, but we have locked before with the same id
            (it, it_meta), *_ = resp

            r = it == locking_id

            print(r, it, locking_id)

            is_locked.set(r)


class LockFree(OpcodeDef):
    name = 'lock_free'

    def fn(self, ctx: ExecutionContext, lock_id: OpArg[str]):
        ctx.singleton.db.delete(f'/locks/{lock_id.get()}')
