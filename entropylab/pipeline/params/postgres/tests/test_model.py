import pytest
from sqlalchemy import create_engine

from entropylab.pipeline.params.postgres.model import Base


# TODO: Remove before PR:
@pytest.skip("for local dev purposes.")
def test_db_creation():
    engine = create_engine(
        "postgresql://test_param_store:kf7yFdNYVjtQQ9H6j5QB@localhost/paramstore1"
    )
    Base.metadata.create_all(engine)
