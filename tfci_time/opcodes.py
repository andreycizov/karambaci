from tfci.opcode import OpcodeDef, OpArg, RefOpArg
from tfci.dsm.executor import ExecutionContext


class WaitOpcode(OpcodeDef):
    name = 'time_wait'

    def fn(self, ctx: ExecutionContext, id: OpArg[str], frz_id: OpArg[str], sleep_time: OpArg[int]):
        pass


class CancelOpcode(OpcodeDef):
    name = 'time_cancel'

    def fn(self, ctx: ExecutionContext, id: OpArg[str]):
        pass
