import grpc
from etcd3.etcdrpc import rpc_pb2, RangeRequest


def _get_secure_creds(ca_cert, cert_key=None, cert_cert=None):
    cert_key_file = None
    cert_cert_file = None

    with open(ca_cert, 'rb') as f:
        ca_cert_file = f.read()

    if cert_key is not None:
        with open(cert_key, 'rb') as f:
            cert_key_file = f.read()

    if cert_cert is not None:
        with open(cert_cert, 'rb') as f:
            cert_cert_file = f.read()

    return grpc.ssl_channel_credentials(
        ca_cert_file,
        cert_key_file,
        cert_cert_file
    )


def build_channel(host, port, ca_cert, cert_key, cert_cert) -> grpc.Channel:
    _url = '{host}:{port}'.format(host=host, port=port)

    cert_params = [c is not None for c in (cert_cert, cert_key)]
    if ca_cert is not None:
        if all(cert_params):
            credentials = _get_secure_creds(
                ca_cert,
                cert_key,
                cert_cert
            )
            channel = grpc.secure_channel(_url, credentials)
        elif any(cert_params):
            # some of the cert parameters are set
            raise ValueError(
                'to use a secure channel ca_cert is required by itself, '
                'or cert_cert and cert_key must both be specified.')
        else:
            credentials = _get_secure_creds(ca_cert, None, None)
            channel = grpc.secure_channel(_url, credentials)
    else:
        channel = grpc.insecure_channel(_url)

    return channel


class Watcher:
    """
    So a Watcher is <preferably> a separate process
    """

    def __init__(self):
        stub = rpc_pb2.WatchStub(build_channel('a', 'b', 'c', 'd', 'e'))


class Connection:
    def __init__(self, urls):
        # connection needs to allow someone to
        pass

        RangeRequest.CREATE
