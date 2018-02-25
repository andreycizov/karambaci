import json
import logging
import time
from argparse import ArgumentParser
from datetime import datetime, timedelta
from typing import NamedTuple
from uuid import uuid4

import etcd3

import tfci.settings
from tfci.time import time_now, ISO8601, time_parse


class DaemonStatus(NamedTuple):
    name: str
    version: str
    ident: str
    started: datetime

    def serialize(self):
        return json.dumps(
            {'name': self.name, 'version': self.version, 'ident': self.ident, 'started': f'{self.started:{ISO8601}}'})

    @classmethod
    def deserialize(cls, x):
        x = json.loads(x)
        return DaemonStatus(
            x['name'],
            x['version'],
            x['ident'],
            time_parse(x['started'], ISO8601),
        )


class Daemon:
    name = 'default'
    version = '0.0.1'
    description = 'default daemon'
    is_daemon = True

    def __init__(self, settings: 'tfci.settings.Settings', **kwargs):
        self.settings = settings
        self.ident = uuid4().hex
        self.lease = None  # type: etcd3.Lease
        self.lease_time = 20
        self.lease_last = None
        self.lease_threshold = 1.
        self.time_started = time_now()
        self.time_lease = None
        self._db = None  # type: etcd3.Etcd3Client

    @property
    def db(self):
        if self._db is None:
            self._db = self.settings.get_db()
        return self._db

    @property
    def ident_key(self):
        return f'/daemons/{self.name}/{self.ident}'

    @property
    def lease_wait_max(self):
        if self.lease_last:
            r = (time_now() - self.lease_last).total_seconds()
        else:
            r = timedelta(days=365).total_seconds()

        r += self.lease_threshold

        r = max(0., self.lease_time - r)

        return r

    def logger(self, name=''):
        return self.settings.get_logger().getChild(name)

    @classmethod
    def arguments(cls, args: ArgumentParser):
        pass

    def get_status(self):
        return DaemonStatus(
            self.name,
            self.version,
            self.ident,
            self.time_started
        )

    def startup(self):
        self.settings.setup_logging()

        if self.is_daemon:

            self.logger().info(f'Daemon {self.ident_key} started up')
            self.lease = self.db.lease(self.lease_time, lease_id=hash(self.ident_key))

            self.db.put(
                self.ident_key,
                self.get_status().serialize(),
                lease=self.lease
            )

    def lease_renew(self, should_log=True):
        self.lease_last = time_now()
        resp = self.lease.refresh()[0]
        if should_log:
            self.logger().info(f'Remaining lease TTL: {resp.TTL}')

    def run(self):
        """
         while True:
            self.logger().info('Ping')
            self.lease_renew()
            self.lease.refresh()
            time.sleep(1)
            for x in self.settings.get_db().get_prefix('/daemons/'):
                print(x)
        """
        raise NotImplementedError('')

    def teardown(self):
        if self.is_daemon:
            self.logger().info('Exitting')
            self.lease.revoke()
            self.logger().info('Exited')
