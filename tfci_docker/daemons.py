import argparse
from argparse import ArgumentParser
from uuid import uuid4

from tfci.daemon import Daemon
from tfci_docker.struct import ServerDef, Auth, CertAuth


class ConfDaemon(Daemon):
    name = 'conf'
    description = 'docker configurator'

    def __init__(self, utility, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.utility = utility
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def arguments(cls, args: ArgumentParser):
        utility_args = args.add_subparsers(
            title="utility",
            dest="utility",
            help="select the utility to run",
            metavar='UTILITY',
        )
        utility_args.required = True

        util_list = utility_args.add_parser('ls', help=f'list servers')
        util_add = utility_args.add_parser('add', help=f'add a server')
        util_test = utility_args.add_parser('test', help=f'test a server')



        util_rm = utility_args.add_parser('rm', help=f'remove a server')

        util_test.add_argument(
            dest='id',
        )

        util_rm.add_argument(
            dest='id',
        )

        util_add.add_argument(
            '-I',
            dest='id',
            required=False,
            default=None,
            help="ID to use for idempotent deployment (leave blank for auto)"
        )

        util_add.add_argument(
            '-P',
            dest='port',
            type=int,
            default=2376,
            help="server port (DEFAULT: %(default)s)"
        )

        util_add.add_argument(
            dest='host',
        )

        auth_sub = util_add.add_subparsers(
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

    def run(self):
        if self.utility == 'ls':
            print('LISTING')
            for k, v in ServerDef.load_all(self.db).items():
                print(k, v)
        elif self.utility == 'add':
            id = self.kwargs['id']
            host = self.kwargs['host']
            port = self.kwargs['port']

            auth_type = self.kwargs['auth_type']

            auth = None

            if auth_type == Auth.Tls.value:
                ca_cert = self.kwargs['ca_cert'].read()
                client_cert = self.kwargs['client_cert'].read()
                client_key = self.kwargs['client_key'].read()

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

            check, mod = srvr.create(self.db)

            ok, _ = self.db.transaction(compare=[check], success=[mod], failure=[])

            if ok:
                print('OK')
            else:
                print('Failure')
        elif self.utility == 'rm':
            check, mod = ServerDef.delete(self.db, self.kwargs['id'])

            ok, _ = self.db.transaction(compare=[check], success=[mod], failure=[])

            if ok:
                print('OK')
            else:
                print('Failure')
        elif self.utility == 'test':
            check, mod = ServerDef.load(self.db, self.kwargs['id'])

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
            raise NotImplementedError(self.utility)
