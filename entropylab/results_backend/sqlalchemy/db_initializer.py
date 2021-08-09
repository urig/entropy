import os
from pathlib import Path

import sqlalchemy.engine
from alembic import script, command
from alembic.config import Config
from alembic.runtime import migration

from entropylab.results_backend.sqlalchemy.model import Base


class DbInitializer:

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self._engine = engine

    def init_db(self) -> None:
        if self._db_is_empty():
            Base.metadata.create_all(self._engine)
            # TODO: Stamp with alembic
        elif not self._db_is_up_to_date():
            raise Exception('Database is not up-to-date. Upgrade the database using update_db().')

    def _db_is_empty(self) -> bool:
        cursor = self._engine.execute("SELECT sql FROM sqlite_master WHERE type = 'table'")
        return len(cursor.fetchall()) == 0

    def _db_is_up_to_date(self) -> bool:
        script_location = self.__abs_path_to("alembic")
        script_ = script.ScriptDirectory(script_location)
        with self._engine.begin() as conn:
            context = migration.MigrationContext.configure(conn)
            db_version = context.get_current_revision()
            latest_version = script_.get_current_head()
            return db_version == latest_version

    def __abs_path_to(self, rel_path: str) -> str:
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        return os.path.join(source_dir, rel_path)

    def update_db(self) -> None:
        # TODO: Test that this doesn't blow up when in memory
        self._alembic_upgrade()
        self._migrate_results_to_hdf5()

    def _alembic_upgrade(self) -> None:
        config_location = self.__abs_path_to("alembic.ini")
        script_location = self.__abs_path_to("alembic")
        alembic_cfg = Config(config_location)
        alembic_cfg.set_main_option('script_location', script_location)
        alembic_cfg.set_main_option('sqlalchemy.url', str(self._engine.url))
        command.upgrade(alembic_cfg, 'head')

    def _migrate_results_to_hdf5(self):
        pass
