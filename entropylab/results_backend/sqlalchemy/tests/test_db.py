import os

import pytest

from entropylab import SqlAlchemyDB, RawResultData
from entropylab.results_backend.sqlalchemy.results_db import HDF_FILENAME


def test_save_result_raises_when_same_result_saved_twice(request):
    # arrange
    path = f"./tests_cache/{request.node.name}.db"
    try:
        db = SqlAlchemyDB(path, echo=True)
        db.__SAVE_RESULTS_IN_HDF5 = True
        raw_result = RawResultData(stage=1, label="foo", data=42)
        db.save_result(0, raw_result)
        with pytest.raises(ValueError):
            # act & assert
            db.save_result(0, raw_result)
    finally:
        # clean up
        _delete_if_exists(HDF_FILENAME)
        _delete_if_exists(path)


def _delete_if_exists(filename: str):
    if os.path.isfile(filename):
        os.remove(filename)
