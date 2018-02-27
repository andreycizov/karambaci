import argparse
from argparse import ArgumentParser
from uuid import uuid4

from tfci.daemon import Daemon
from tfci.mapper import EntityManager
from tfci_docker.struct import ServerDef, Auth, CertAuth


class ServerDefManager(EntityManager):
    entity = ServerDef.__name__
    model = ServerDef

    @classmethod
    def _arguments_else(cls, args: ArgumentParser, utility_args):
        util_test = utility_args.add_parser('test', help=f'test a server')

        util_test.add_argument(
            dest='id',
        )

    @classmethod
    def arguments_add(cls, args: ArgumentParser):
        super().arguments_add(args)

        args.add_argument(
            '-P',
            dest='port',
            type=int,
            default=2376,
            help="server port (DEFAULT: %(default)s)"
        )

        args.add_argument(
            dest='host',
        )

        auth_sub = args.add_subparsers(
            title='auth',
            dest='auth_type',
            help='select the authentication type',
            metavar="AUTH",
        )
        auth_sub.required = True

        auth_tls = auth_sub.add_parser(Auth.Tls.value, help=f'TLS authentication')

        auth_tls.add_argument(
            dest='ca_cert',
            type=argparse.FileType('r'),
            help='CA certificate file',
        )

        auth_tls.add_argument(
            dest='client_cert',
            type=argparse.FileType('r'),
            help='client certificate file',
        )

        auth_tls.add_argument(
            dest='client_key',
            type=argparse.FileType('r'),
            help='client key file',
        )

    def action_add(self, **kwargs):
        id = kwargs['id']
        host = kwargs['host']
        port = kwargs['port']

        auth_type = kwargs['auth_type']

        auth = None

        if auth_type == Auth.Tls.value:
            ca_cert = kwargs['ca_cert'].read()
            client_cert = kwargs['client_cert'].read()
            client_key = kwargs['client_key'].read()

            auth = CertAuth(ca_cert, client_cert, client_key)
        else:
            raise NotImplementedError(f'{auth_type}')

        if id is None:
            id = uuid4().hex
            print('Generated a new ID', id)

        srvr = ServerDef.new(
            id,
            auth,
            host,
            port
        )

        return srvr.create(self.db)

    def _action_else(self, utility, **kwargs):
        if utility == 'test':
            check, mod = self.model.load(self.db, kwargs['id'])

            ok, ret = self.db.transaction(compare=[check], success=[mod], failure=[])

            if ok:
                item, = ret

                x = ServerDef.deserialize_range(item)

                assert x is not None

                with x.client() as dckr:
                    print(dckr.version())
            else:
                print('Failure')
        else:
            raise NotImplementedError('')


class ConfDaemon(Daemon):
    name = 'conf'
    description = 'docker configurator'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.utility = utility
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def arguments(cls, args: ArgumentParser):
        ServerDefManager.arguments(args)

    def run(self):
        ServerDefManager(self.db).action(**self.kwargs)
