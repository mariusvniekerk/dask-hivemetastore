# Sample Test passing with nose and pytest
from contextlib import closing

from dask_hivemetastore.hive_metastore import connect, DaskMetaStoreWrapper
import pyhive.hive
from pytest import fixture
import os
from six.moves import urllib
import pandas as pd
from pandas.testing import assert_frame_equal


class DaskMetaStoreWrapperRemap(DaskMetaStoreWrapper):

    def _remap_path(self, url):
        pwd = os.path.abspath(os.curdir)
        parsed = urllib.parse.urlparse(url)
        new_path = parsed.path.replace('/data/hive/warehouse', os.path.join(pwd, 'data', 'warehouse'))
        new_url = parsed._replace(path=os.path.join(new_path, '*'))
        # if new_url.scheme == 'file':
        #     new_url = new_url._replace(scheme='')
        return urllib.parse.urlunparse(new_url)


@fixture(scope='module')
def client():
    client = connect('127.0.0.1', ifclass=DaskMetaStoreWrapperRemap)
    return client

@fixture(scope='module')
def hive():
    conn = pyhive.hive.connect('127.0.0.1', port=10000, auth='NONE')
    return conn.cursor()

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
        assert_frame_equal(pd_df.set_index('a'), target.set_index('a'), )

    finally:
        hive.execute(
            "DROP TABLE test_csv"
        )
