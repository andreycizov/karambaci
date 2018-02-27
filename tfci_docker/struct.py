import json
import os
import tempfile
from enum import Enum
from typing import NamedTuple, Union

import docker
from docker.tls import TLSConfig

from tfci.db.mapper import MapperBase, NamedTupleEx


class Auth(Enum):
    Tls = 'tls'


class CertAuth(NamedTuple):
    ca: str
    cert: str
    key: str


AUTH_MAP = {
    Auth.Tls: CertAuth
}

AUTH_MAP_REV = {v: k for k, v in AUTH_MAP.items()}


class ServerDef(NamedTupleEx, MapperBase):
    id: str
    auth: Union[CertAuth]
    host: str
    port: int
    version: int

    def __str__(self):
        return f'ServerDef(id={self.id}, auth={self.auth_type.value}, host={self.host}, port={self.port}, version={self.version})'

    @classmethod
    def new(cls, id, auth, host, port):
        return ServerDef(id, auth, host, port, -1)

    @classmethod
    def key_fn(cls, id):
        return f'/docker/servers/{id}'

    @property
    def auth_type(self) -> Auth:
        return AUTH_MAP_REV[self.auth.__class__]

    def serialize(self):
        return json.dumps([[self.auth_type.value, self.auth._asdict()], self.host, self.port])

    @classmethod
    def deserialize(cls, id, version, bts):
        (auth_type, auth_val), *other = json.loads(bts)

        auth_cls = AUTH_MAP[Auth(auth_type)]

        auth = auth_cls(**auth_val)

        return ServerDef(id, auth, *other, version)

    def client(self):
        return DockerWrapper(self)


def create_temp_content(text):
    f_h, name = tempfile.mkstemp(text=True)
    file_obj = os.fdopen(f_h, mode='w+')
    file_obj.write(text)
    file_obj.close()
    return name


class DockerWrapper:
    def __init__(self, d: ServerDef):
        self.d = d
        self.temps = []

    def __enter__(self):
        kwargs = {}

        if self.d.auth_type == Auth.Tls:
            ca = create_temp_content(self.d.auth.ca)
            cc = create_temp_content(self.d.auth.cert)
            ck = create_temp_content(self.d.auth.key)

            kwargs['tls'] = TLSConfig(
                client_cert=(cc, ck), ca_cert=ca,
                assert_hostname=True,
            )

            self.temps = [ca, cc, ck]
        else:
            raise NotImplementedError(self.d.auth_type)

        return docker.DockerClient(
            base_url=f'tcp://{self.d.host}:{self.d.port}',
            timeout=1,
            **kwargs
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        for x in self.temps:
            os.unlink(x)
