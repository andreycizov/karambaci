from tfci.opcode import OpcodeDef, RefOpArg, OpArg
from tfci.dsm.executor import ExecutionContext, ExecutionError
from tfci_docker.struct import ServerDef


def load_server(ctx: ExecutionContext, serv_id: str) -> ServerDef:
    compare, succ = ServerDef.load_exists(ctx.singleton.db, serv_id)

    ok, (serv, *_) = ctx.singleton.db.transaction(compare=[compare], success=[succ], failure=[])

    if not ok:
        raise ExecutionError(f'Can not load server `{serv_id}`')

    server = ServerDef.deserialize_range(serv)

    if server is None:
        raise ExecutionError(f'Can not load server `{serv_id}`')

    return server


class PullOpcode(OpcodeDef):
    name = 'dckr_pull'

    def fn(self, ctx: ExecutionContext, serv_id: OpArg[str], image: OpArg[str], tag: OpArg[str]):
        server = load_server(ctx, serv_id.get())

        with server.client() as c:
            c.images.pull(image.get(), tag=tag.get())


class StartOpcode(OpcodeDef):
    name = 'dckr_start'

    def fn(self, ctx: ExecutionContext, serv_id: OpArg[str], name: OpArg[str], image: OpArg[str], *args: OpArg[str]):
        server = load_server(ctx, serv_id.get())

        with server.client() as c:
            c.containers.run(
                image=image.get(),
                name=name.get(),
                detach=True
            )


class WaitOpcode(OpcodeDef):
    name = 'dckr_wait'

    # e.g. dckr_wait "locust" "live" frozen_id status="STOPPED"

    def fn(self, ctx: ExecutionContext, *args, **kwargs):
        ctx.singleton.settings.get_daemons()
        raise NotImplementedError('')


class RemoveOpcode(OpcodeDef):
    name = 'dckr_rm'

    def fn(self, ctx: ExecutionContext, *args, **kwargs):
        ctx.singleton.settings.get_daemons()
        raise NotImplementedError('')
