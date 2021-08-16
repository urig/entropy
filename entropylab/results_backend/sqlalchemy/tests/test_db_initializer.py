import os
from datetime import datetime
from shutil import copyfile

from entropylab import SqlAlchemyDB, RawResultData
from entropylab.results_backend.sqlalchemy.results_db import HDF_FILENAME, HDF5ResultsDB
from entropylab.results_backend.sqlalchemy.db_initializer import _DbInitializer


def test_upgrade_db_when_initial_db_is_empty(request):
    try:
        # arrange
        db_template = f"./db_templates/initial.db"
        db_under_test = _get_test_file_name(db_template)
        copyfile(db_template, db_under_test)
        target = _DbInitializer(db_under_test, echo=True)
        # act
        target.upgrade_db()
        # assert
        cur = target._engine.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'Results'"
        )
        res = cur.fetchone()
        cur.close()
        assert "saved_in_hdf5" in res[0]
    finally:
        # clean up
        _delete_if_exists(HDF_FILENAME)
        _delete_if_exists(db_under_test)


def test_upgrade_db_when_db_is_in_memory(request):
    target = _DbInitializer(":memory:", echo=True)
    # act
    target.upgrade_db()
    # assert
    cur = target._engine.execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
    res = cur.fetchone()
    cur.close()
    assert "saved_in_hdf5" in res[0]


def test__migrate_results_to_hdf5(request):
    try:
        path = f"./tests_cache/{request.node.name}.db"
        # arrange
        db = SqlAlchemyDB(path, echo=True)
        db.__SAVE_RESULTS_IN_HDF5 = False
        db.save_result(1, RawResultData(stage=1, label="foo", data="bar"))
        db.save_result(1, RawResultData(stage=1, label="baz", data="buz"))
        db.save_result(1, RawResultData(stage=2, label="biz", data="bez"))
        db.save_result(2, RawResultData(stage=1, label="bat", data="bot"))
        db.save_result(3, RawResultData(stage=1, label="ooh", data="aah"))
        target = _DbInitializer(path, echo=True)
        # act
        target._migrate_results_to_hdf5()
        # assert
        results_db = HDF5ResultsDB()
        hdf5_results = results_db.get_results()
        assert len(hdf5_results) == 5
        cur = target._engine.execute("SELECT * FROM Results WHERE saved_in_hdf5 = 1")
        res = cur.all()
        assert len(res) == 5
    finally:
        # clean up
        _delete_if_exists(HDF_FILENAME)
        _delete_if_exists(path)


def _get_test_file_name(filename):
    timestamp = f"{datetime.now():%Y-%m-%d-%H-%M-%S}"
    return filename.replace("db_templates", "tests_cache").replace(
        ".db", f"_{timestamp}.db"
    )


def _delete_if_exists(filename: str):
    if os.path.isfile(filename):
        os.remove(filename)
