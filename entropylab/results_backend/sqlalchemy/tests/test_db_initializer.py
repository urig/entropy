import os

from sqlalchemy import create_engine

from config import settings
from entropylab import SqlAlchemyDB, RawResultData
from entropylab.api.data_writer import Metadata
from entropylab.results_backend.sqlalchemy.storage import HDF5Storage
from entropylab.results_backend.sqlalchemy.db_initializer import (
    _DbInitializer,
    _ENTROPY_DIRNAME,
    _HDF5_FILENAME,
    _DbUpgrader,
)
from entropylab.results_backend.sqlalchemy.tests.test_utils import (
    delete_if_exists,
    create_test_project,
)


def test_upgrade_db_when_initial_db_is_empty(request):
    # arrange
    test_project_dir = create_test_project(request, f"./db_templates/initial.db")
    try:
        target = _DbInitializer()
        # act
        target.upgrade_db(test_project_dir, echo=False)
        # assert
        cur = create_engine(
            f"sqlite:///{test_project_dir}/.entropy/entropy.db"
        ).execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
        res = cur.fetchone()
        cur.close()
        assert "saved_in_hdf5" in res[0]
    finally:
        # clean up
        delete_if_exists(test_project_dir)


def test_upgrade_db_when_db_is_in_memory():
    try:
        # arrange
        target = _DbInitializer()
        target.init_db(":memory:", echo=True)
        # act
        target.upgrade_db(":memory:", echo=True)
        # assert
        cur = target._engine.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'Results'"
        )
        res = cur.fetchone()
        cur.close()
        assert "saved_in_hdf5" in res[0]
    finally:
        # clean up
        delete_if_exists("./entropy.hdf5")


def test__migrate_results_to_hdf5(request):
    # arrange
    settings.toggles = {"hdf5_storage": False}
    test_project_dir = create_test_project(request)
    try:
        # save to DB but not to storage:
        db = SqlAlchemyDB(test_project_dir, echo=True, enable_hdf5_storage=False)
        db.save_result(1, RawResultData(stage=1, label="foo", data="bar"))
        db.save_result(1, RawResultData(stage=1, label="baz", data="buz"))
        db.save_result(1, RawResultData(stage=2, label="biz", data="bez"))
        db.save_result(2, RawResultData(stage=1, label="bat", data="bot"))
        db.save_result(3, RawResultData(stage=1, label="ooh", data="aah"))
        # target = _DbInitializer(test_project_dir, echo=True)
        target = _DbUpgrader(test_project_dir, echo=True)
        # act
        target._migrate_results_to_hdf5()
        # assert
        storage = HDF5Storage(
            os.path.join(test_project_dir, _ENTROPY_DIRNAME, _HDF5_FILENAME)
        )
        hdf5_results = storage.get_result_records()
        assert len(list(hdf5_results)) == 5
        cur = target._engine.execute("SELECT * FROM Results WHERE saved_in_hdf5 = 1")
        res = cur.all()
        assert len(res) == 5
    finally:
        # clean up
        delete_if_exists(test_project_dir)


def test__migrate_metadata_to_hdf5(request):
    # arrange
    test_project_dir = create_test_project(request)
    try:
        # save to DB but not to storage:
        db = SqlAlchemyDB(test_project_dir, echo=True, enable_hdf5_storage=False)
        db.save_metadata(1, Metadata(stage=1, label="foo", data="bar"))
        db.save_metadata(1, Metadata(stage=1, label="baz", data="buz"))
        db.save_metadata(1, Metadata(stage=2, label="biz", data="bez"))
        db.save_metadata(2, Metadata(stage=1, label="bat", data="bot"))
        db.save_metadata(3, Metadata(stage=1, label="ooh", data="aah"))
        target = _DbUpgrader(test_project_dir, echo=True)
        # act
        target._migrate_metadata_to_hdf5()
        # assert
        storage = HDF5Storage(
            os.path.join(test_project_dir, _ENTROPY_DIRNAME, _HDF5_FILENAME)
        )
        hdf5_metadata = storage.get_metadata_records()
        assert len(list(hdf5_metadata)) == 5
        cur = target._engine.execute(
            "SELECT * FROM ExperimentMetadata WHERE saved_in_hdf5 = 1"
        )
        res = cur.all()
        assert len(res) == 5
    finally:
        # clean up
        delete_if_exists(test_project_dir)
