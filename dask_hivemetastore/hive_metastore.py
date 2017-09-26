from __future__ import absolute_import

import logging as log
from dask_hivemetastore._thrift_api import (
    get_socket, get_transport, TBinaryProtocol, ThriftClient, Table, StorageDescriptor, SerDeInfo, FieldSchema)
from dask.dataframe import read_csv, read_parquet, read_table, concat, DataFrame


if False:
    from typing import *


class HiveCompatibleFormat(object):

    def __init__(self, input_format, output_format, serde):
        self.input_format = input_format,
        self.output_format = output_format
        self.serde = serde

PARQUET = HiveCompatibleFormat(
    input_format=['org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'],
    output_format=['org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'],
    serde=[
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
        # other deprecated formats are here
        'parquet.hive.serde.ParquetHiveSerDe',
    ]
)


DELIMITED = HiveCompatibleFormat(
    input_format=['org.apache.hadoop.mapred.TextInputFormat'],
    output_format=['org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'],
    serde=[
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "org.apache.hadoop.hive.serde2.OpenCSVSerde",
    ]
)


def hive_type_to_dtype(hive_typ):
    raise NotImplementedError


class DaskMetaStoreWrapper(object):

    def __init__(self, service):
        assert isinstance(service, ThriftClient)
        self.service = service

    def get_table_info(self, tablename, database=None):
        # type: (str, Optional[str]) -> Table
        if database is None:
            database = 'default'
        return self.service.get_table(database, tablename)

    def table_to_dask(self, tablename, columns=None, database=None, **kwargs):
        info = self.get_table_info(tablename, database)
        sd = info.sd  # type: StorageDescriptor
        # for some types inputFormat denotes what we need to specify for compression settings
        file_location = sd.location
        input_format = sd.inputFormat
        serde_info = sd.serdeInfo  # type: SerDeInfo
        # denotes parquet / csv / avro
        if serde_info.serializationLib in PARQUET.serde:
            return self._parquet_table_to_dask(file_location, columns)
        elif serde_info.serializationLib in DELIMITED.serde:
            return self._delimited_table_to_dask(file_location, serde_info, sd.cols, info.parameters)

    def _parquet_table_to_dask(self, storage_location, columns):
        return read_parquet(storage_location, columns=columns)

    def _delimited_table_to_dask(self, storage_location, serde_info, columns, table_properties, selected_columns=None):
        # type: (str, SerDeInfo, List[FieldSchema]) -> DataFrame

        if serde_info.serializationLib == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe":
            kwargs = dict(
                escapechar=serde_info.parameters.get('escape.delim'),
                lineterminator=serde_info.parameters.get('line.delim'),
                delimiter=serde_info.parameters.get('field.delim')
            )
        elif serde_info.serializationLib == "org.apache.hadoop.hive.serde2.OpenCSVSerde":
            kwargs = dict(
                delimiter=serde_info.parameters.get("seperatorChar", ","),
                quotechar=serde_info.parameters.get("quoteChar", '"'),
            )
        else:
            raise ValueError("Unexpected serde: {}".format(serde_info.serializationLib))

        skip_header_lines = int(table_properties.get("skip.header.line.count", 0))
        dd = read_csv(
            storage_location,
            names=[c.name for c in columns],
            dtype={c.name: hive_type_to_dtype(c) for c in columns},
            header=skip_header_lines,
            **kwargs)
        if selected_columns is not None:
            return dd[selected_columns]
        else:
            return dd


def connect(host, port=9083, timeout=None, use_ssl=False, ca_cert=None,
            user=None, password=None, kerberos_service_name='impala',
            auth_mechanism=None):
    """Connect to a Hive metastore

    """
    log.debug('Connecting to Hive metastore %s:%s with %s authentication '
              'mechanism', host, port, auth_mechanism)
    sock = get_socket(host, port, use_ssl, ca_cert)
    if timeout is not None:
        timeout = timeout * 1000.  # TSocket expects millis
    sock.setTimeout(timeout)
    transport = get_transport(sock, host, kerberos_service_name,
                              auth_mechanism, user, password)
    transport.open()
    protocol = TBinaryProtocol(transport)
    service = ThriftClient(protocol)
    log.debug('sock=%s transport=%s protocol=%s service=%s', sock, transport,
              protocol, service)
    return DaskMetaStoreWrapper(service)
