# Sample Test passing with nose and pytest
import getpass
import time
import os

from thrift.transport.TTransport import TTransportException

from dask_hivemetastore.hive_metastore import connect, DaskMetaStoreWrapper
import pyhive.hive
from pytest import fixture
from six.moves import urllib
import pandas as pd
from pandas.testing import assert_frame_equal
from dockerctx import new_container, accepting_connections
from docker import DockerClient
from shutil import rmtree

class DaskMetaStoreWrapperRemap(DaskMetaStoreWrapper):

    def _remap_path(self, url, extension=''):
        parsed = urllib.parse.urlparse(url)
        new_path = parsed.path.replace('/data/hive/warehouse', os.path.join(self.hive_data_dir, 'warehouse'))
        new_url = parsed._replace(path=new_path)
        return super()._remap_path(urllib.parse.urlunparse(new_url), extension)


@fixture(scope='session')
def hive_data_dir(tmpdir_factory):
    hive_data = tmpdir_factory.mktemp('hive')
    yield hive_data
    rmtree(hive_data, ignore_errors=True)


@fixture(scope='session')
def docker_client():
    client = DockerClient.from_env()
    yield client


@fixture(scope='session')
def hms(hive_data_dir):
    with new_container(
            'fpin/docker-hive-spark',
            ports={
                '9083/tcp': None,
                '10000/tcp': None,
                  },
            entrypoint='/start.sh',
            environment={
                'USER': getpass.getuser(),
                'USER_ID': os.getuid(),
            },
            volumes={
                hive_data_dir: '/data/hive'
            }
            ) as container:
        # according to the container this needs a bit of sleep time.
        time.sleep(30)
        yield container
        logs = container.logs()
        print("\n\nLogs from docker process")
        print(logs.decode('utf-8'))


def get_container_ports(client: DockerClient, id: str, port: str):
    res = client.api.inspect_container(id)
    return res['NetworkSettings']['Ports'][port][0]['HostPort']


@fixture(scope='session')
def clients(hms, docker_client, hive_data_dir):
    SLEEP_TIME = 10

    mapped_port = get_container_ports(docker_client, hms.id, '9083/tcp')
    accepting_connections(host='127.0.0.1', port=mapped_port)
    mapped_port_thrift = get_container_ports(docker_client, hms.id, '10000/tcp')
    accepting_connections(host='127.0.0.1', port=mapped_port_thrift)
    for i in range(10):
        try:
            client = connect('127.0.0.1', port=mapped_port, ifclass=DaskMetaStoreWrapperRemap)
            if client.status() == 'ALIVE':
                client.hive_data_dir = hive_data_dir
            print("Connected HMS")
            conn = pyhive.hive.connect('127.0.0.1', port=mapped_port_thrift, auth='NONE')
            hive = conn.cursor()
            print(f"Connected after {i * SLEEP_TIME} seconds")
            return (client, hive)

        except TTransportException as e:
            print(f"Attempt {i:02d}: Failed {repr(e.message)}")
            time.sleep(SLEEP_TIME)
        except BrokenPipeError as e:
            print(f"Attempt {i:02d}: Failed {repr(e)}")
            time.sleep(SLEEP_TIME)
    else:
        logs = hms.logs()
        print("\n\nLogs from docker process")
        print(logs.decode('utf-8'))
        raise TimeoutError


@fixture(scope='session')
def client(clients):
    return clients[0]


@fixture(scope='session')
def hive(clients):
    return clients[1]


def test_status(client):
    res = client.status()
    assert res == 'ALIVE'


def test_pass(client):
    assert client.service.get_all_databases() == ['default']


def test_create_drop(hive):
    hive.execute(
        "CREATE TABLE a(a INT, b STRING) STORED AS PARQUET"
    )

    hive.execute(
        "DROP TABLE a"
    )


def test_csv(hive, client):
    try:
        hive.execute(r"""
            CREATE TABLE test_csv(a INT, b STRING) 
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
            """)

        # Two inserts will ensure two dask partitions.
        hive.execute("""
            INSERT INTO test_csv
            VALUES (1, '1')
        """)
        hive.execute("""
            INSERT INTO test_csv
            VALUES (2, '2')
        """)

        hive.fetchall()
        dd = client.table_to_dask('test_csv')
        pd_df = dd.compute()

        target = pd.DataFrame(data={'a': [1, 2], 'b': ['1', '2']}, columns=['a', 'b'])
        # validate that these are the same
        assert_frame_equal(pd_df.set_index('a').sort_index(), target.set_index('a').sort_index())

    finally:
        hive.execute(
            "DROP TABLE test_csv"
        )


def test_parquet(hive, client):
    table_name = 'test_parquet'
    try:
        hive.execute(f"""
            CREATE TABLE {table_name}(a INT, b STRING) 
            STORED AS PARQUET
            """)

        # Two inserts will ensure two dask partitions.
        hive.execute(f"""
            INSERT INTO {table_name}
            VALUES (1, '1')
        """)
        hive.execute(f"""
            INSERT INTO {table_name}
            VALUES (2, '2')
        """)

        hive.fetchall()
        dd = client.table_to_dask(table_name)
        pd_df = dd.compute()

        target = pd.DataFrame(data={'a': [1, 2], 'b': ['1', '2']}, columns=['a', 'b'])
        # validate that these are the same
        assert_frame_equal(pd_df.set_index('a').sort_index(), target.set_index('a').sort_index())

    finally:
        hive.execute(
            f"DROP TABLE {table_name}"
        )
