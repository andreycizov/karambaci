import logging
import multiprocessing
import queue
from enum import Enum
from multiprocessing import Queue
from typing import NamedTuple, Iterator, List, Union, Type, Optional

from etcd3 import utils, Etcd3Client
from etcd3.events import DeleteEvent, PutEvent
from etcd3.transactions import BaseCompare, Put, Get, Delete

from tfci.db.mapper import MapperBase

logger = logging.getLogger(__name__)


def watch_range(db, key, cb):
    kwargs = {}
    kwargs['range_end'] = \
        utils.increment_last_byte(utils.to_bytes(key))

    def cb1(*args):
        cb(key, *args)

    return db.add_watch_callback(key=key, callback=cb1, **kwargs)


class Ev(Enum):
    Delete = 'DELETE'
    Put = 'PUT'


class RangeEvent(NamedTuple):
    prefix: str
    ident: str
    event: Ev
    body: bytes
    version: int

    @property
    def key(self):
        return self.prefix + self.ident


def watch_range_queue(queue: Queue, db: Etcd3Client, prefix):
    kwargs = {}
    kwargs['range_end'] = \
        utils.increment_last_byte(utils.to_bytes(prefix))

    def cb(event):
        ident = event.key.decode()[len(prefix):]

        if isinstance(event, DeleteEvent):
            queue.put(RangeEvent(prefix, ident, Ev.Delete, event.value, event.version))
        elif isinstance(event, PutEvent):
            queue.put(RangeEvent(prefix, ident, Ev.Put, event.value, event.version))
        else:
            logger.error(f'UnknownEvent: {event}')

    return db.add_watch_callback(key=prefix, callback=cb, **kwargs)


def queue_try(route_queue) -> Iterator[RangeEvent]:
    while True:
        try:
            yield route_queue.get(timeout=0)
        except (multiprocessing.TimeoutError, queue.Empty) as e:
            return


TX_COMPARE = BaseCompare
TX_MOD = Union[Put, Delete, Get]


class Transaction(NamedTuple):
    compare: List[BaseCompare]
    success: List[TX_MOD]
    failure: List[TX_MOD]

    @classmethod
    def new(cls, compare=None, success=None, failure=None):
        if compare is None:
            compare = []

        if success is None:
            success = []

        if failure is None:
            failure = []

        return Transaction(compare, success, failure)

    def __and__(self, o: 'Transaction'):
        return Transaction(
            self.compare + o.compare,
            self.success + o.success,
            self.failure + o.failure,
        )

    def exec(
        self,
        db: Etcd3Client,
        ok_map: Optional[List[Optional[Type[MapperBase]]]] = None,
        fail_map: Optional[List[Optional[Type[MapperBase]]]] = None
    ):
        ok, items = db.transaction(
            compare=self.compare,
            success=self.success,
            failure=self.failure,
        )

        if ok and ok_map:
            assert len(items) == len(ok_map)

            return ok, [y.deserialize_range(z) for z, y in zip(items, ok_map) if y]
        if not ok and fail_map:
            assert len(items) == len(fail_map)

            return ok, [y.deserialize_range(z) for z, y in zip(items, fail_map) if y]

        return ok, items


class Watch(NamedTuple):
    db: Etcd3Client
    queue: queue.Queue

    @classmethod
    def new(cls, db):
        return Watch(db, queue.Queue())

    def register(self, prefix) -> 'WatchToken':
        return WatchToken(self.db, self.queue, watch_range_queue(self.queue, self.db, prefix))


class WatchToken(NamedTuple):
    db: Etcd3Client
    queue: queue.Queue
    id: int

    @classmethod
    def new(cls, db, queue, id):
        return WatchToken(db, queue, id)

    def get(self, timeout=0, close=True) -> RangeEvent:
        try:
            r = self.queue.get(timeout=timeout)

            return r
        except queue.Empty:
            raise TimeoutError('Queue had reached a timeout')
        finally:
            if close:
                self.db.cancel_watch(self.id)


def watch_range_single(db: Etcd3Client, prefix):
    event_queue = queue.Queue()

    watch_id = watch_range_queue(event_queue, db, prefix)

    return WatchToken(db, event_queue, watch_id)
