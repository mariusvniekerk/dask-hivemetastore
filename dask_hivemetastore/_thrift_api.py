from __future__ import absolute_import

import logging as log
import getpass

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import (
    TBufferedTransport, TTransportException)
from thrift.Thrift import TApplicationException
from thrift.protocol.TBinaryProtocol import (
    TBinaryProtocolAccelerated as TBinaryProtocol)

from dask_hivemetastore._gen.hive_metastore.ThriftHiveMetastore import Client as ThriftClient
from dask_hivemetastore._gen.hive_metastore.ttypes import (
    Table,
    Partition,
    StorageDescriptor,
    FieldSchema,
    SerDeInfo
)


def get_socket(host, port, use_ssl, ca_cert):
    # based on the Impala shell impl
    log.debug('get_socket: host=%s port=%s use_ssl=%s ca_cert=%s',
              host, port, use_ssl, ca_cert)
    if use_ssl:
        from thrift.transport.TSSLSocket import TSSLSocket
        if ca_cert is None:
            return TSSLSocket(host, port, validate=False)
        else:
            return TSSLSocket(host, port, validate=True, ca_certs=ca_cert)
    else:
        return TSocket(host, port)


def get_transport(socket, host, kerberos_service_name, auth_mechanism='NOSASL',
                  user=None, password=None):
    """
    Creates a new Thrift Transport using the specified auth_mechanism.
    Supported auth_mechanisms are:
    - None or 'NOSASL' - returns simple buffered transport (default)
    - 'PLAIN'  - returns a SASL transport with the PLAIN mechanism
    - 'GSSAPI' - returns a SASL transport with the GSSAPI mechanism
    """
    log.debug('get_transport: socket=%s host=%s kerberos_service_name=%s '
              'auth_mechanism=%s user=%s password=fuggetaboutit', socket, host,
              kerberos_service_name, auth_mechanism, user)

    if auth_mechanism == 'NOSASL':
        return TBufferedTransport(socket)

    # Set defaults for PLAIN SASL / LDAP connections.
    if auth_mechanism in ['LDAP', 'PLAIN']:
        if user is None:
            user = getpass.getuser()
            log.debug('get_transport: user=%s', user)
        if password is None:
            if auth_mechanism == 'LDAP':
                password = ''
            else:
                # PLAIN always requires a password for HS2.
                password = 'password'
            log.debug('get_transport: password=%s', password)

    # Initializes a sasl client
    from thrift_sasl import TSaslClientTransport
    try:
        import sasl  # pylint: disable=import-error

        def sasl_factory():
            sasl_client = sasl.Client()
            sasl_client.setAttr('host', host)
            sasl_client.setAttr('service', kerberos_service_name)
            if auth_mechanism.upper() in ['PLAIN', 'LDAP']:
                sasl_client.setAttr('username', user)
                sasl_client.setAttr('password', password)
            sasl_client.init()
            return sasl_client
    except ImportError:
        log.warn("Unable to import 'sasl'. Fallback to 'puresasl'.")
        from dask_hivemetastore.sasl_compat import PureSASLClient

        def sasl_factory():
            return PureSASLClient(host, username=user, password=password,
                                  service=kerberos_service_name)

    return TSaslClientTransport(sasl_factory, auth_mechanism, socket)
