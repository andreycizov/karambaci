import logging
import multiprocessing
import queue
from enum import Enum
from multiprocessing import Queue
from typing import NamedTuple, Iterator

from etcd3 import utils, Etcd3Client, exceptions
from etcd3.events import DeleteEvent, PutEvent, Event

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



class WatchToken(NamedTuple):
    db: Etcd3Client
    queue: queue.Queue
    id: int

    @classmethod
    def new(cls, db, queue, id):
        return WatchToken(db, queue, id)

    def get(self, timeout=0, close=True) -> RangeEvent:
        try:
            return self.queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError('')
        finally:
            if close:
                self.db.cancel_watch(self.id)


def watch_range_single(db: Etcd3Client, prefix):
    event_queue = queue.Queue()

    watch_id = watch_range_queue(event_queue, db, prefix)

    return WatchToken(db, event_queue, watch_id)
