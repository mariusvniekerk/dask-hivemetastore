from __future__ import absolute_import

import logging as log
from dask_hivemetastore._thrift_api import (
    get_socket, get_transport, TBinaryProtocol, ThriftClient, Table, StorageDescriptor, SerDeInfo, FieldSchema,
    Partition)
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
    """

    The type information from hive is stored as a string.

    These have the following forms

    PRIMITIVE_TYPE
    COMPLEX_TYPE<PARAMS>

    For now we consider only primitive types.
    """
    raise NotImplementedError


class DaskMetaStoreWrapper(object):
    """Utility wrapper class that can extract the minimal required information from the hive metastore in order to
    create a dask dataframe.

    Don't construct this class directly use :meth:`connect` instead.

    """

    def __init__(self, service):
        # type: (ThriftClient) -> None
        self.service = service

    def _get_table_info(self, tablename, database=None):
        # type: (str, Optional[str]) -> Table
        if database is None:
            database = 'default'
        return self.service.get_table(database, tablename)

    def _get_partitions(self, tablename, database=None, partition_filter=None):
        # type: (str, Optional[str], Optional[str]) -> List[Partition]
        if database is None:
            database = 'default'
        if partition_filter is not None:
            partitions = self.service.get_partitions_by_filter(database, tablename, partition_filter, max_parts=-1)
        else:
            partitions = self.service.get_partitions(database, tablename, max_parts=-1)
        return partitions

    def table_to_dask(
            self,
            tablename,              # type: str
            columns=None,           # type: Optional[List[str]]
            database=None,          # type: Optional[str]
            partition_filter=None,  # type: Optional[str]
            **kwargs
            ):
        # type: (...) -> DataFrame
        """

        :param tablename:           Name of the hive table to load
        :param columns:             Column names to load, optional
        :param database:            Database (schema) that the table is in
        :param partition_filter:    For partitioned tables specify a filter that partitions must adhere to as a string
        :param kwargs:              Other arguments that are passed down to the underlying loaders
        :return:
        """
        info = self._get_table_info(tablename, database)
        sd = info.sd                                # type: StorageDescriptor
        serde_info = sd.serdeInfo                   # type: SerDeInfo
        partition_keys = info.partitionKeys         # type: List[FieldSchema]

        if len(info.partitionKeys):
            partitions = self._get_partitions(database, partition_filter, tablename)

            # walk the list of partitions and concatenate the resulting dataframe
            dataframes = []
            for partition in partitions:
                sd = partition.sd                   # type: StorageDescriptor
                dd = self._hive_metastore_data_to_dask(
                    location=sd.location,
                    serde_info=sd.serdeInfo,
                    metastore_columns=sd.cols,
                    table_params=sd.parameters,
                    kwargs=kwargs
                )
                # TODO: convert the dtypes of the fields to the correct one.
                for col, part_key in zip(partition_keys, partition.values):
                    params = {col.name: part_key}
                    dd = dd.assign(**params)
                dataframes.append(dd)
            return concat(dataframes)
        else:
            return self._hive_metastore_data_to_dask(
                location=sd.location,
                serde_info=serde_info,
                metastore_columns=sd.cols,
                table_params=sd.parameters,
                kwargs=kwargs
            )

    def _hive_metastore_data_to_dask(
            self,
            serde_info,             # type: SerDeInfo
            location,               # type: str
            metastore_columns,      # type: List[FieldSchema]
            table_params,           # type: Mapping[str, str]
            column_subset=None,     # type: Optional[List[str]]
            kwargs=None,            # type: Optional[Dict[str, Any]]
            ):
        # type: (...) -> DataFrame
        kwargs = kwargs or {}
        if serde_info.serializationLib in PARQUET.serde:
            return self._parquet_table_to_dask(location, column_subset, kwargs=kwargs)

        elif serde_info.serializationLib in DELIMITED.serde:
            return self._delimited_table_to_dask(
                location=location,
                serde_info=serde_info,
                columns=metastore_columns,
                table_properties=table_params,
                selected_columns=column_subset,
                kwargs=kwargs
            )

    def _parquet_table_to_dask(
            self,
            location,               # type: str
            column_subset,          # type: Optional[List[str]]
            kwargs,                 # type: Dict[str, Any]
        ):
        # type: (...) -> DataFrame
        return read_parquet(location, columns=column_subset, **kwargs)

    def _delimited_table_to_dask(
            self,
            serde_info,             # type: SerDeInfo
            location,               # type: str
            metastore_columns,      # type: List[FieldSchema]
            table_params,           # type: Mapping[str, str]
            column_subset,          # type: Optional[List[str]]
            kwargs,                 # type: Dict[str, Any]
            ):
        # type: (...) -> DataFrame
        if serde_info.serializationLib == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe":
            csv_args = dict(
                escapechar=serde_info.parameters.get('escape.delim'),
                lineterminator=serde_info.parameters.get('line.delim'),
                delimiter=serde_info.parameters.get('field.delim')
            )
        elif serde_info.serializationLib == "org.apache.hadoop.hive.serde2.OpenCSVSerde":
            csv_args = dict(
                delimiter=serde_info.parameters.get("seperatorChar", ","),
                quotechar=serde_info.parameters.get("quoteChar", '"'),
            )
        else:
            raise ValueError("Unexpected serde: {}".format(serde_info.serializationLib))

        kwargs.update(csv_args)
        skip_header_lines = int(table_params.get("skip.header.line.count", 0))
        dd = read_csv(
            location,
            names=[c.name for c in metastore_columns],
            dtype={c.name: hive_type_to_dtype(c) for c in metastore_columns},
            header=skip_header_lines,
            **kwargs)
        if column_subset is not None:
            return dd[column_subset]
        else:
            return dd


def connect(host, port=9083, timeout=None, use_ssl=False, ca_cert=None, user=None, password=None,
            kerberos_service_name='hive_metastore', auth_mechanism=None):
    """Connect to a Hive metastore and return a dask compatibility wrapper.

    """
    log.debug('Connecting to Hive metastore %s:%s with %s authentication '
              'mechanism', host, port, auth_mechanism)
    sock = get_socket(host, port, use_ssl, ca_cert)
    if timeout is not None:
        timeout = timeout * 1000.  # TSocket expects millis
    sock.setTimeout(timeout)
    transport = get_transport(sock, host, kerberos_service_name, auth_mechanism, user, password)
    transport.open()
    protocol = TBinaryProtocol(transport)
    service = ThriftClient(protocol)
    log.debug('sock=%s transport=%s protocol=%s service=%s', sock, transport, protocol, service)
    return DaskMetaStoreWrapper(service)
