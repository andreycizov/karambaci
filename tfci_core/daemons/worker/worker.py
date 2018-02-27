import logging
from typing import Tuple, Optional

from etcd3 import Lease, Etcd3Client

from tfci.dsl.compiler import load_test_program
from tfci.dsl.exception import CompilerException
from tfci.dsl.struct import ProgramDefinition
from tfci.dsm.executor import ExecutionError, ExecutionSingleton, ExecutionContext
from tfci.dsm.struct import FollowUp, ThreadContext
from tfci_core.daemons.generic.pool import WorkerInstance
from tfci.settings import Settings

logger = logging.getLogger(__name__)


class ThreadExecutorInstance(WorkerInstance):
    def __init__(self, proc_ident, ident, lease_id, settings: Settings):
        super().__init__()
        self.proc_ident = proc_ident
        self.ident = ident
        self.lease = Lease(lease_id, None)
        self.settings = settings

        self.opcodes = None
        self.singleton = ExecutionSingleton(self.settings)
        self.db = None  # type: Etcd3Client
        self.program = None  # type: ProgramDefinition

    def startup(self):
        self.settings.setup_logging()
        logger.info(f'TaskExecutor {self.ident} {self.proc_ident} startup')
        self.db = self.settings.get_db()

        self.opcodes = {x.name: x for x in self.settings.get_opcodes()}
        self.p_filename, self.p_text, self.program = load_test_program(self.opcodes)

    def gen_trace(self, pdi, stack, thread):
        print('TRACE', self.ident, thread.id, thread.ip, pdi.opcode, pdi.args, pdi.next_label)
        for s in stack:
            print('\t', s)
        print('ENDTRACE')

    def gen_exc(self, pdi, stack, tid, tip, thread_orig, tsp):
        logger.exception(f'ExecutionError TID=`{tid}` IP=`{tip}` OSP={thread_orig.sp} SP={tsp} Stacks={stack}')

        if pdi:
            logger.error('CODE')
            x = CompilerException(pdi.loc, "The exception happened here:").with_text(self.p_text).with_filename(
                self.p_filename)

            logger.error(str(x))
            logger.error('CODE_NOT')
        if stack:
            logger.error('STACK')

            for s in stack:
                logger.error(f'\t{s}')

            logger.error('STACK_NOT')

    def __call__(self, thread_id, thread_orig: ThreadContext) -> Tuple[bool, Optional[ThreadContext]]:
        # purpose: test the executor.

        # the issue is that a ThreadExecutorInstance must not be able to reload the code at all (?)
        # it needs to be able to use a centralised cache located in the daemon

        ok, thread, stack = thread_orig.lock(self.db, self.ident, self.lease)

        tid = thread.id if thread else thread_id
        tip = thread.ip if thread else None
        tsp = thread.sp if thread else None
        pdi = self.program[tip] if tip else None


        try:
            if not ok:
                logger.error(f'Thread `{thread_id}` lock failed')
                return False, None

            if not thread:
                raise ExecutionError(f'Thread `{thread_id}` could not be found')

            if thread.ip not in self.program:
                raise ExecutionError(f'Thread `{thread.id}` IP=`{thread.ip}` could not be found')

            self.gen_trace(pdi, stack, thread)

            # that's where we essentially execute anything.

            f = self.opcodes[pdi.opcode]()(
                ExecutionContext(
                    self.singleton,
                    pdi.args,
                    pdi.next_label,
                    thread,
                    stack
                )
            )  # type: FollowUp

            # FollowUp is not execution context.

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
