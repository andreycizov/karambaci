import time
import unittest
from itertools import count
from typing import Optional

from etcd3wrapper.rpc import KV, RangeRequest, WatchCreateRequest, WatchRequest, WatchCancelRequest, Watch
from tfci.db.conn import _get_secure_creds, build_channel

pars = {
    'h': 'locust.bpmms.com',
    'p': 2379,
    'ca': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/ca.crt',
    'cert': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.crt',
    'key': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.key.insecure',
    't': 10,
}


def _build_chan():
    c = build_channel(
        pars['h'],
        pars['p'],
        pars['ca'],
        pars['key'],
        pars['cert'],
    )
    return c


class TestDB(unittest.TestCase):
    def test_simple(self):

        x = KV(_build_chan()).Range(RangeRequest(
            key=b'\0',
            range_end=b'\0',
        ))

        for k in x.kvs:
            pass

    def test_iter(self):
        watch_id: Optional[int] = None
        should_start = False

        def requester():
            nonlocal should_start
            nonlocal watch_id
            yield WatchRequest(
                create_request=WatchCreateRequest(
                    key=b'\0',
                    range_end=b'\0',
                    progress_notify=True
                )
            )

            for _ in count():
                if watch_id is not None:
                    yield WatchRequest(
                        cancel_request=WatchCancelRequest(
                            watch_id=watch_id
                        )
                    )
                    should_start = True
                    watch_id = None
                elif should_start:
                    should_start = False
                    yield WatchRequest(
                        create_request=WatchCreateRequest(
                            key=b'\0',
                            range_end=b'\0',
                            progress_notify=True
                        )
                    )
                else:
                    time.sleep(0.25)

        iter_obj = Watch(_build_chan()).Watch(
            requester()
        )

        for i, x in enumerate(iter_obj):
            watch_id = x.watch_id

            if i == 10:
                break
