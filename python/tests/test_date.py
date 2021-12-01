import sys
import pytest
import pyarrow as pa
import pandas as pd
import numpy as np
import omniscidbe as dbe
import ctypes
from unittest import mock
ctypes._dlopen('libDBEngine.so', ctypes.RTLD_GLOBAL)

sys.setdlopenflags(1 | 256)

def test_init():
    global engine
    engine = dbe.PyDbEngine()
    assert bool(engine.closed) == False

engine = None


def test_arrow_schema_convertion():
    arrow_schema = pa.schema({'string': pa.string(), 'int8': pa.int8(), 'int16': pa.int16(), 'int32': pa.int32(), 'int64': pa.int64(), 'float': pa.float32(), 'double': pa.float64(), 'bool': pa.bool_(), 'timestamp_s': pa.timestamp('s'), 'timestamp_ns': pa.timestamp('ns')})
    df = pd.DataFrame({'string': list('abcdefghij'), 'int8': np.random.randint(0, 100, size=(10,)), 'int16': np.random.randint(0, 100, size=(10,)), 'int32': np.random.randint(0, 100, size=(10,)), 'int64': np.random.randint(0, 100, size=(10,)), 'float': np.random.randint(0, 100, size=(10,)), 'double': np.random.randint(0, 100, size=(10,)), 'bool': np.random.randint(0, 100, size=(10,)), 'timestamp_s': np.random.randint(0, 100, size=(10,)), 'timestamp_ns': np.random.randint(0, 100, size=(10,))})
    table = pa.Table.from_pandas(df, schema=arrow_schema)
    assert table
    test_name = "test_table"
    engine.importArrowTable(test_name, table)
    assert bool(engine.closed) == False
    cursor = engine.executeDML("select * from {}".format(test_name))
    assert cursor
    batch = cursor.getArrowRecordBatch()
    assert batch
    

if __name__ == "__main__":
    pytest.main(["-v", __file__])
