import logging
import multiprocessing
import queue
import signal
from argparse import ArgumentParser
from uuid import uuid4

from etcd3.events import DeleteEvent, PutEvent

from tfci.daemon import Daemon
from tfci_core.daemons.worker.worker import ThreadExecutorInstance
from tfci_core.const import JOBS_THREAD, JOBS_LOCK
from tfci_core.daemons.db_util import watch_range
from tfci_core.daemons.generic.pool import TaskProcessPool
from tfci_core.daemons.worker.struct import ThreadContext

logger = logging.getLogger(__name__)


class WorkerDaemon(Daemon):
    name = 'worker'
    version = '0.0.1'
    description = 'task queue support'

    def __init__(self, parallel, **kwargs):
        super().__init__(**kwargs)
        self.parallel = parallel
        self.manager = multiprocessing.Manager()

        self.daemons = {}
        self.ctx = {}
        self.lock = {}
        self.watches = []

        self.route_queue = self.manager.Queue()  # type: Queue
        self.running = True
        self.pool = None

    @classmethod
    def arguments(cls, args: ArgumentParser):
        args.add_argument(
            '-P',
            '--parallel',
            dest='parallel',
            default=2,
            help='How many processes in parallel to use for ?'
        )

    def startup(self):
        super().startup()

    def ctx_put(self, key, val):
        the_ctx = ThreadContext.deserialize(key, val)
        self.ctx[key] = the_ctx

        self.ctx_lock_change(key)

    def ctx_delete(self, key):
        self.settings.get_logger().debug(f'CTX={key}: DELETED')
        if key in self.ctx:
            del self.ctx[key]

    def lock_put(self, key, val):
        self.lock[key] = val

    def lock_delete(self, key):
        if key in self.lock:
            #self.settings.get_logger().debug(f'LockID={key}: RELEASED')
            del self.lock[key]

            if key in self.ctx:
                self.ctx_lock_change(key)
            # todo: if lock had been removed and we're still working on this task - then kill the task.
        else:
            #self.settings.get_logger().error(f'LockID={key}: UNKNOWN')

            if key in self.ctx:
                self.ctx_lock_change(key)

    def ctx_lock_change(self, key):
        if self.lock.get(key):
            #self.settings.get_logger().info(f'Ignoring {key} -> LOCK SET')
            return

        if not self.pool.available:
            #self.settings.get_logger().info(f'Ignoring {key} -> POOL FULL')
            return

        if key not in self.pool and key in self.ctx:
            #self.settings.get_logger().info(f'Assigned {key}')
            self.pool.assign(key, self.ctx[key])

    def ctx_pool_fill(self):
        ctxs = [x for x in self.ctx.keys() if x not in self.lock]

        # logger.info(f'{self.pool.available} {len(ctxs)} {self.lease_wait_max}')

        while self.pool.available > 0 and len(ctxs) and self.lease_wait_max > 0:
            item = ctxs.pop()
            self.ctx_lock_change(item)

    def events_process(self):
        try:
            (cb, *args) = self.route_queue.get(timeout=0)
            getattr(self, cb)(*args)
        except (multiprocessing.TimeoutError, queue.Empty) as e:
            return

    def get_prefix(self, key, fn):
        for y, x in self.db.get_prefix(key):
            fn(x.key.decode()[len(key):], y.decode())

    def daemon_put(self, key, value):
        self.daemons[key] = value

    def daemon_delete(self, key):
        print(self.daemons)
        del self.daemons[key]

    def watch_restart(self):
        def create_cb(q, p, d):
            p = p.__name__
            d = d.__name__

            def cb(prefix, event):
                ident = event.key.decode()[len(prefix):]

                print(prefix, event)

                if isinstance(event, DeleteEvent):

                    q.put((d, ident))
                elif isinstance(event, PutEvent):
                    q.put((p, ident, event.value.decode()))
                else:
                    self.settings.get_logger().error(f'UnknownEvent: {event}')

            return cb

        self.watch_stop()

        watch_id1 = watch_range(self.db, JOBS_THREAD % ('',),
                                create_cb(self.route_queue, self.ctx_put, self.ctx_delete))
        watch_id2 = watch_range(self.db, JOBS_LOCK % ('',),
                                create_cb(self.route_queue, self.lock_put, self.lock_delete))
        watch_daemons = watch_range(self.db, f'/daemons/{self.name}/',
                                    create_cb(self.route_queue, self.daemon_put, self.daemon_delete))

        self.watches = [watch_id1, watch_id2, watch_daemons]

    # def task_handle(self, task_id, task):
    #

    def watch_stop(self):
        for w in self.watches:
            self.db.cancel_watch(w)
        self.watches = []

    def stop(self):
        self.settings.get_logger().getChild('cli.daemon').debug('STOPPING')
        self.running = False

    def run(self):
        signal.signal(signal.SIGINT, lambda a, b: self.stop())

        self.pool = TaskProcessPool(
            # lambda: self.manager.Queue(),
            self.parallel,
            ThreadExecutorInstance,
            (self.ident, self.lease.id, self.settings)
        )

        self.get_prefix('/daemons/', self.daemon_put)
        self.get_prefix(JOBS_LOCK % ('',), self.lock_put)
        self.get_prefix(JOBS_THREAD % ('',), self.ctx_put)
        self.watch_restart()

        while self.running:
            polling = True
            while polling and self.running:
                self.events_process()
                # todo: we also need to check if every of the subprocesses is still running!

                self.ctx_pool_fill()

                x = self.pool.poll(min(0.5, self.lease_wait_max))

                if self.lease_wait_max < self.lease_threshold:
                    polling = False

                if x:
                    task_id, is_ok, task_result = x

                    if is_ok:
                        (ok, thread_obj) = task_result

                        if ok:
                            if thread_obj is None and task_id in self.ctx:
                                del self.ctx[task_id]
                            elif thread_obj is not None:
                                self.ctx[thread_obj.id] = thread_obj
                            logger.error(f'Task OK {task_id}')
                        else:
                            logger.error(f'Task FAIL {task_id}')
                    else:
                        logger.error(f'Exception raised in subtask: {task_result}')

            self.lease_renew()

    def teardown(self):
        self.watch_stop()
        if self.pool:
            self.pool.close()
        super().teardown()


class JobsDaemon(Daemon):
    name = 'jobs'
    version = '0.0.1'
    description = 'task queue support'

    @classmethod
    def arguments(cls, args):
        pass

    def run(self):
        db = self.settings.get_db()

        for i in range(1):
            ctx = ThreadContext(uuid4().hex, 'entrypoint', [])
            db.put(ctx.key, ctx.serialize())

            self.lease_renew(False)
