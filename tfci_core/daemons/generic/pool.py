import base64
import logging
import multiprocessing
import multiprocessing.connection
import pickle
import setproctitle
import signal
import subprocess
import typing
from uuid import uuid4

logger = logging.getLogger(__name__)


def argv_encode(x):
    return base64.b64encode(pickle.dumps(x)).decode()


def argv_decode(x):
    return pickle.loads(base64.b64decode(x))


def task_process_pool_process(ident, addr, cls, cls_args):
    running = True

    def stop(*args):
        nonlocal running
        running = False

    # signal.signal(signal.SIGINT, stop)
    setproctitle.setproctitle(f'pool-{ident}')

    cls = argv_decode(cls)
    cls_args = argv_decode(cls_args)

    with multiprocessing.connection.Client(addr, family='AF_UNIX') as c:
        singleton = cls(ident, *cls_args)

        singleton.startup()
        try:
            while running:
                (task_id, task) = c.recv()

                try:
                    rtn = singleton(task_id, task)
                except Exception as e:
                    logger.exception(f'Error happened {e}')
                    try:
                        c.send((task_id, False, e))
                    except:
                        # logger.exception('Could not serialize exception')
                        c.send((task_id, False, None))
                else:
                    c.send((task_id, True, rtn))
        except KeyboardInterrupt:
            pass
        except:
            logger.exception('')


class WorkerInstance:
    def __init__(self, *args, **kwargs):
        pass

    def startup(self):
        pass

    def __call__(self, task_id, payload):
        pass


class TaskProcessRecord:
    def __init__(self, q: multiprocessing.connection.Connection, p: subprocess.Popen):
        self.queue = q
        self.process = p
        self.is_dead = False

    def recv(self):
        try:
            return self.queue.recv()
        except EOFError:
            self.is_dead = True

    def send(self, item: typing.Any):
        try:
            self.queue.send(item)
        except BrokenPipeError:
            self.is_dead = True


class TaskProcessPool:
    def __init__(self, parallel, cls: typing.Type[WorkerInstance], cls_args):
        self.listener = multiprocessing.connection.Listener(family='AF_UNIX')

        self.parallel = parallel
        self.cls = cls
        self.cls_args = cls_args

        self.processes = {}  # type: typing.Dict[int, TaskProcessRecord]
        self.free = []
        self.assigned = {}

        for x in range(self.parallel):
            self.process_start()

    def process_start(self):
        ident = uuid4().hex
        import sys

        args = [
            sys.executable,
            '-m',
            __name__,
            ident,
            self.listener.address,
            argv_encode(self.cls),
            argv_encode(self.cls_args)
        ]

        # p = subprocess.Popen(args, stdout=sys.stdout, stderr=sys.stderr)
        p = subprocess.Popen(args)
        q = self.listener.accept()

        logger.debug(f'Process started: {ident}')

        self.processes[ident] = TaskProcessRecord(q, p)
        self.free.append(ident)

    @property
    def available(self):
        return len(self.free)

    def __contains__(self, item):
        return item in self.assigned

    def assign(self, task_id, task):
        assert self.available > 0, "Must be available"

        proc_id = self.free.pop()

        logger.info(f'Assigning {task_id}: {list(self.assigned.keys())}')

        self.assigned[task_id] = proc_id
        self.processes[proc_id].send((task_id, task))

    def resign(self, task_id):
        assert task_id in self.assigned, f"Must be assigned: TaskID={task_id}"

        proc_id = self.assigned[task_id]
        del self.assigned[task_id]
        self.free.append(proc_id)
        return proc_id

    def cancel(self, task_id):
        proc_id = self.resign(task_id)

        self.processes[proc_id].process.terminate()
        del self.processes[proc_id]
        self.process_start()

    def poll(self, timeout=5):
        map = {x.queue: x for k, x in self.processes.items()}

        items = multiprocessing.connection.wait(list(map.keys()), timeout=timeout)

        items = [map[x] for x in items]

        if len(items) == 0:
            return None

        x, *items = items

        x = x.recv()

        if x is None:
            return None

        task_id, ok, task_rtn = x

        proc_id = self.resign(task_id)

        return task_id, ok, task_rtn

    def close(self):
        for x in self.processes.values():
            logger.info(f'Killing {x.process.pid}')
            x.queue.close()
            x.process.kill()
        for x in self.processes.values():
            logger.info(f'Waiting {x.process.pid}')
            x.process.wait()
        logger.info(f'All processes are clean now')
        self.listener.close()
        logger.info(f'Listener closed as well')


if __name__ == '__main__':
    import sys

    task_process_pool_process(*sys.argv[1:])
