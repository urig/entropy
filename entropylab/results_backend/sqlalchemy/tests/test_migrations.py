import os
from datetime import datetime
from shutil import copyfile

import pytest

from entropylab import SqlAlchemyDB


@pytest.mark.parametrize(
    "path", [
        None,
        ":memory:"
    ])
def test_ctor_creates_up_to_date_schema_when_in_memory(path: str):
    # act
    target = SqlAlchemyDB(path=path, echo=True)
    # assert
    cur = target._engine.execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
    res = cur.fetchone()
    assert "saved_in_hdf5" in res[0]


@pytest.mark.parametrize(
    "db_template, expected_to_throw", [
        (None, False),  # new db
        ("db_templates/empty.db", False),  # existing but empty
        ("db_templates/initial.db", True),  # revision 1318a586f31d
        ("db_templates/with_saved_in_hdf5_col.db", False)  # revision 04ae19b32c08
    ])
def test_ctor_ensures_latest_migration(db_template: str, expected_to_throw: bool):
    # arrange
    if db_template is not None:
        db_under_test = get_test_file_name(db_template)
        copyfile(db_template, db_under_test)
    else:
        db_under_test = get_test_file_name("tests_cache/new.db")
    try:
        if expected_to_throw:
            with pytest.raises(Exception):
                # act & assert
                SqlAlchemyDB(path=db_under_test, echo=True)
        else:
            SqlAlchemyDB(path=db_under_test, echo=True)
    finally:
        # clean up
        os.remove(db_under_test)


# Add a test for db with initial schema and values in tables.
# test revision matrix


def get_test_file_name(filename):
    timestamp = f"{datetime.now():%Y-%m-%d-%H-%M-%S}"
    return filename \
        .replace("db_templates", "tests_cache") \
        .replace(".db", f"_{timestamp}.db")
