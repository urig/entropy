import uuid

from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import expression

Base = declarative_base()


class UtcNow(expression.FunctionElement):
    """SqlAlchemy function to generate UTC timestamp on server-side (Postgres)
    Source: https://docs.sqlalchemy.org/en/14/core/compiler.html#utc-timestamp-function
    """

    type = DateTime()
    inherit_cache = True


@compiles(UtcNow, "postgresql")
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


class Commit(Base):
    __tablename__ = "commit"
    commit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(
        DateTime(timezone=False), nullable=False, server_default=UtcNow()
    )
    label = Column(String(256))
    params = Column(JSONB, nullable=False)
    tags = Column(JSONB, nullable=False)

    def __repr__(self):
        return f"<Commit(commit_id={self.id}, label={self.label})>"
