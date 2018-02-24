from tfci.opcode import OpcodeDef, OpArg
from tfci.dsm.executor import ExecutionContext
from tfci_http.struct import Reply


class ReplyOpcode(OpcodeDef):
    name = 'http_rep'

    def fn(self, ctx: ExecutionContext, req_id: OpArg[str], **kwargs: OpArg[str]):
        db = ctx.singleton.db

        body = {k: v.get() for k, v in kwargs.items()}

        rep = Reply(req_id.get(), body)

        ok, _ = db.transaction(
            compare=[
                db.transactions.version(rep.key) == 0
            ],
            success=[
                db.transactions.put(rep.key, rep.serialize())
            ], failure=[

            ]
        )

