from tfci_core.plugin import CorePlugin
from tfci_docker.plugin import DockerPlugin
from tfci_http.plugin import HTTPPlugin
from tfci_std.plugin import StdPlugin
from tfci_time.plugin import TimePlugin
from tfci.settings import Settings

T = 1.

OPTS = Settings(
    [
        CorePlugin(),
        DockerPlugin(),
        HTTPPlugin(),
        TimePlugin(),
        StdPlugin()
    ],
    [
        {
            'h': 'locust.bpmms.com',
            'p': 2379,
            'ca': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/ca.crt',
            'cert': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.crt',
            'key': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.key.insecure',
            't': T,
        },
        {
            'h': 'orchard.bpmms.com',
            'p': 2379,
            'ca': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/ca.crt',
            'cert': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.crt',
            'key': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.key.insecure',
            't': T,
        },
        {
            'h': 'orchard2.bpmms.com',
            'p': 2379,
            'ca': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/ca.crt',
            'cert': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.crt',
            'key': '/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.key.insecure',
            't': T,
        },
    ]
)
