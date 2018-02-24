import json
import os
import tempfile
from abc import ABCMeta, abstractmethod, abstractclassmethod
from enum import Enum
from json import JSONDecodeError
from typing import NamedTuple, Union, Dict, Optional, TypeVar, Type

import docker
from docker.tls import TLSConfig
from etcd3 import Etcd3Client


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

T = TypeVar('T')


class ORMMeta:
    __metaclass__ = ABCMeta

    @classmethod
    @abstractmethod
    def key_fn(self, id) -> str:
        return f'/{id}'

    @property
    def key(self):
        return self.key_fn(self.id)

    @classmethod
    def delete(cls, db: Etcd3Client, id):
        return db.transactions.version(cls.key_fn(id)) > 0, db.transactions.delete(cls.key_fn(id))

    @classmethod
    def load(cls, db: Etcd3Client, id):
        return db.transactions.version(cls.key_fn(id)) > 0, db.transactions.get(cls.key_fn(id))

    @abstractmethod
    def serialize(self) -> str:
        return ''

    @classmethod
    def deserialize(cls: Type[T], id, version, bts) -> T:
        return cls()

    @classmethod
    def deserialize_range(cls: Type[T], items) -> Optional[T]:
        if items is None:
            return None
        elif len(items) == 0:
            return None
        elif len(items) > 1:
            assert False, f'you are deserializing a range that is too long? {range}'

        (it, meta), *_ = items

        id = meta.key.decode()[len(cls.key_fn('')):]
        return cls.deserialize(id, meta.version, it)

    @classmethod
    def load_all(cls: Type[T], db: Etcd3Client) -> Dict[str, T]:
        r = {}

        prefix = cls.key_fn('')

        for v, v_m in db.get_prefix(prefix):
            k = v_m.key.decode()[len(prefix):]
            try:
                r[k] = cls.deserialize(k, v_m.version, v)
            except Exception as e:
                raise ValueError(f'While deserializing ID={k}: {e}')

        return r


class ServerDef(NamedTuple, ORMMeta):
    id: str

    auth: Union[CertAuth]

    host: str
    port: int
    version: int

    def __str__(self):
        return f'ServerDef(id={self.id}, auth={self.auth_type.value}, host={self.host}, port={self.port}, version={self.version})'

    @classmethod
    def key_fn(cls, id):
        return f'/docker/servers/{id}'

    @property
    def auth_type(self) -> Auth:
        return AUTH_MAP_REV[self.auth.__class__]

    @property
    def key(self):
        return self.key_fn(self.id)

    def serialize(self):
        return json.dumps([[self.auth_type.value, self.auth._asdict()], self.host, self.port])

    @classmethod
    def deserialize(cls, id, version, bts):
        (auth_type, auth_val), *other = json.loads(bts)

        auth_cls = AUTH_MAP[Auth(auth_type)]

        auth = auth_cls(**auth_val)

        return ServerDef(id, auth, *other, version)

    @classmethod
    def new(cls, id, auth, host, port):
        return ServerDef(id, auth, host, port, -1)

    def create(self, db: Etcd3Client):
        return db.transactions.version(self.key) == 0, db.transactions.put(self.key, self.serialize())

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
