from sqlalchemy import create_engine

from entropylab.pipeline.params.postgres.model import Base

# TODO: Remove before PR:
from entropylab.pipeline.params.postgres.param_store import ParamStore


# @pytest.mark.skip("for local dev purposes.")
def test_db_creation():
    engine = create_engine(
        "postgresql://test_param_store:kf7yFdNYVjtQQ9H6j5QB@localhost/paramstore1"
    )
    Base.metadata.create_all(engine)


def test_commit_postgres():
    target = ParamStore(
        "postgresql://test_param_store:kf7yFdNYVjtQQ9H6j5QB@localhost/paramstore1"
    )
    target["foo"] = "bar"
    commit_id = target.commit("test")
    assert commit_id is not None
