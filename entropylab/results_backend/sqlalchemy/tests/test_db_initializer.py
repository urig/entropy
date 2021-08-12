import os

import entropylab
from entropylab import SqlAlchemyDB, RawResultData
from entropylab.results_backend.sqlalchemy.results_db import HDF_FILENAME, ResultsDB
from entropylab.results_backend.sqlalchemy.db_initializer import _DbInitializer


def test__migrate_results_to_hdf5():
    try:
        entropylab.results_backend._HDF5_RESULTS_DB = False
        # arrange
        db = SqlAlchemyDB("tests_cache/tmp.db", echo=True)
        db.__SAVE_RESULTS_IN_HDF5 = False
        db.save_result(1, RawResultData(stage=1, label="foo", data="bar"))
        db.save_result(1, RawResultData(stage=1, label="baz", data="buz"))
        db.save_result(1, RawResultData(stage=2, label="biz", data="bez"))
        db.save_result(2, RawResultData(stage=1, label="bat", data="bot"))
        db.save_result(3, RawResultData(stage=1, label="ooh", data="aah"))
        target = _DbInitializer(db._engine)
        # act
        target._migrate_results_to_hdf5()
        # assert
        results_db = ResultsDB()
        hdf5_results = results_db.get_results()
        assert len(hdf5_results) == 5
        cur = target._engine.execute("SELECT * FROM Results WHERE saved_in_hdf5 = 1")
        res = cur.all()
        assert len(res) == 5
    finally:
        # clean up
        os.remove(HDF_FILENAME)
        os.remove("tests_cache/tmp.db")
