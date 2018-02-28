import logging
from typing import Tuple, Optional

from etcd3 import Etcd3Client

from tfci.dsl.exception import CompilerException
from tfci.dsm.executor import ExecutionError, ExecutionSingleton, ExecutionContext
from tfci.dsm.struct import FollowUp, ThreadContext
from tfci.dsm.rt import OpcodeDefinition, ProgramPages
from tfci_core.daemons.db_util import Lease
from tfci_core.daemons.generic.pool import WorkerInstance
from tfci.settings import Settings

logger = logging.getLogger(__name__)


class ExecutionEngine:
    def __init__(
        self,
        ident: str,
        lease: Lease,
        opcodes: OpcodeDefinition,
        pages: ProgramPages,
        db: Etcd3Client,
    ):
        self.ident = ident
        self.lease = lease
        self.opcodes = opcodes
        self.pages = pages
        self.db = db

        self.singleton = ExecutionSingleton(self.db)

    def gen_trace(self, pdi, stack, thread):
        print('TRACE', self.ident, thread.id, thread.ip, pdi.opcode, pdi.args, pdi.next_label)
        for s in stack:
            print('\t', s)
        print('ENDTRACE')

    def gen_exc(self, pdi, stack, tid, tip, thread_orig, tsp):
        logger.exception(f'ExecutionError TID=`{tid}` IP=`{tip}` OSP={thread_orig.sp} SP={tsp} Stacks={stack}')

        if pdi:
            logger.error('CODE')

            # we supposedly need to reload the code here, but out-of-bounds.
            x = CompilerException(pdi.loc, "The exception happened here:")
            x = self.pages.decorate_exception(x)

            logger.error(str(x))
            logger.error('CODE_NOT')
        if stack:
            logger.error('STACK')

            for s in stack:
                logger.error(f'\t{s}')

            logger.error('STACK_NOT')

    def step(self, thread_orig: ThreadContext):
        ok, thread, stack = thread_orig.lock(
            self.db,
            self.ident,
            self.lease.to_etcd3()
        )

        tid = thread.id if thread else thread_orig.id
        tip = thread.ip if thread else None
        tsp = thread.sp if thread else None

        # print('A', thread, tip)
        pdi = self.pages[tip] if (tip and tip in self.pages) else None

        try:
            if not ok:
                logger.error(f'Thread `{thread_orig.id}` lock failed')
                return False, None

            if not thread:
                logger.error(f'Thread `{thread_orig.id}` could not be found')
                return False, None

            if thread.ip not in self.pages:
                raise ExecutionError(f'Thread `{thread.id}` IP=`{thread.ip}` could not be found')

            # self.gen_trace(pdi, stack, thread)

            # that's where we essentially execute anything.

            opcode = self.opcodes[pdi.opcode]

            # we may check here is opcode supports idempotency (?) it must

            f = opcode(
                ExecutionContext.new(
                    self.singleton,
                    pdi.args,
                    pdi.next_label,
                    thread,
                    stack,
                )
            )  # type: FollowUp
        except ExecutionError as e:
            thread_orig.follow(
                self.db, self.ident,
                FollowUp.new()
            )

            self.gen_exc(pdi, stack, tid, tip, thread_orig, tsp)
            return False, None
        except:
            # everything that is not an execution error should be just retried
            thread_orig.unlock(self.db, self.ident)
            self.gen_exc(pdi, stack, tid, tip, thread_orig, tsp)
            return False, None
        else:
            ok, updated = thread_orig.follow(
                self.db, self.ident,
                f
            )

        return ok, updated


class ThreadExecutorInstance(WorkerInstance):
    def __init__(self, proc_ident, ident, lease_id, settings: Settings):
        super().__init__()
        self.proc_ident = proc_ident
        self.ident = ident
        self.settings = settings

        db = self.settings.get_db()
        opcodes = self.settings.get_opcodes()

        self.engine = ExecutionEngine(
            self.ident,
            Lease(lease_id),
            opcodes,
            ProgramPages(db, opcodes),
            db
        )

    def startup(self):
        self.settings.setup_logging()
        logger.info(f'TaskExecutor {self.ident} {self.proc_ident} startup')

    def __call__(self, thread_id, thread_orig: ThreadContext) -> Tuple[bool, Optional[ThreadContext]]:
        return self.engine.step(thread_orig)
