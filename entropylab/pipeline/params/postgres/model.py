import uuid

from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Commit(Base):
    __tablename__ = "commit"
    commit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime(timezone=False), nullable=False)
    label = Column(String(256))
    params = Column(JSONB, nullable=False)
    tags = Column(JSONB, nullable=False)

    def __repr__(self):
        return f"<Commit(commit_id={self.id}, label={self.label})>"
