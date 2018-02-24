from pprint import pprint

import etcd3

etcd3.transactions

c = etcd3.client(
    host='locust.bpmms.com',
    port=2379,
    ca_cert='/Users/apple/Projects/BPMMS/SSL/etcd-cluster/ca.crt',
    cert_cert='/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.crt',
    cert_key='/Users/apple/Projects/BPMMS/SSL/etcd-cluster/client1.key.insecure',
)

pprint([str(x) for x in c.members])

c.watch_prefix

c.with_pre

c.transactions
