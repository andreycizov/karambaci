import os
import shutil
import subprocess
import tempfile
import time
import unittest
from typing import Any

from etcd3.exceptions import ConnectionFailedError

from tfci.dsm.executor import ExecutionContext
from tfci.opcode import OpcodeDef, OpArg
from tfci.settings import Settings
from tfci.db.db_util import Lease


def callback_fixture(opcode, callback):
    """
    :param opcode: the opcode that you'll be using in the test dasm
    :param callback: (*args, **kwargs) callable receiving all of the arguments to the opcode
    :return: OpcodeDef(name=opcode)
    """
    class CallbackFixture(OpcodeDef):
        name = opcode

        def __init__(self, callback):
            self.callback = callback
            super().__init__()

        def fn(self, ctx: ExecutionContext, *args: OpArg[Any], **kwargs: OpArg[Any]):
            self.callback(*(x.get() for x in args), **{k: v.get() for k, v in kwargs.items()})

    return CallbackFixture(callback)


class Etcd3ServerFixture(unittest.TestCase):
    etcd3_client_host = f'localhost'
    etcd3_client_port = 2379
    etcd3_path = os.environ.get('ETCD3') or "/Users/apple/Downloads/etcd-v3.3.1-darwin-amd64"

    plugins = []

    @classmethod
    def setUpClass(cls):
        cls.etcd3_dir = tempfile.mkdtemp()
        cls.etcd3_serv = subprocess.Popen(
            [
                os.path.join(cls.etcd3_path, 'etcd'),
                '--data-dir',
                os.path.join(cls.etcd3_dir, 'data'),
                '--wal-dir',
                os.path.join(cls.etcd3_dir, 'wal'),
                '--listen-client-urls',
                f'http://{cls.etcd3_client_host}:{cls.etcd3_client_port}',
                '--advertise-client-urls',
                f'http://{cls.etcd3_client_host}:{cls.etcd3_client_port}',
            ],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        import time
        time.sleep(5)

    def _settings(self):
        return Settings(
            self.plugins,
            [
                {
                    'h': self.etcd3_client_host,
                    'p': self.etcd3_client_port,
                }
            ]
        )

    def _db_lease(self):
        db = self._settings().get_db()

        while True:
            try:
                lease = Lease(db.lease(20).id)
                break
            except ConnectionFailedError:
                print('ConnectionFailed')
                time.sleep(1)
        return db, lease

    @classmethod
    def tearDownClass(cls):
        cls.etcd3_serv.terminate()
        shutil.rmtree(cls.etcd3_dir)
