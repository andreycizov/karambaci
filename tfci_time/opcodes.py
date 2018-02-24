from tfci.opcode import OpcodeDef, OpArg, RefOpArg
from tfci.dsm.executor import ExecutionContext


class NowOpcode(OpcodeDef):
    name = 'time_shift'

    def fn(self, ctx: ExecutionContext, dest: RefOpArg[str], *shift: OpArg[str]):


        pass


class SchedOpcode(OpcodeDef):
    name = 'time_sched'

    def fn(self, ctx: ExecutionContext, dest: OpArg[str], cb: OpArg[str]):
        # create a schedule of something something
        pass


class WaitOpcode(OpcodeDef):
    name = 'time_sleep'

    def fn(self, ctx: ExecutionContext, dest: OpArg[str], frz_id: OpArg[str]):
        pass
