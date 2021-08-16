import os
from pathlib import Path

from alembic import script, command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy import create_engine
import sqlalchemy.engine
from sqlalchemy.orm import sessionmaker

from entropylab.logger import logger
from entropylab.results_backend.sqlalchemy.model import Base, ResultTable
from entropylab.results_backend.sqlalchemy.results_db import HDF5ResultsDB

_SQL_ALCHEMY_MEMORY = ":memory:"


class _DbInitializer:
    def __init__(self, path: str, echo=False):
        if path is None:
            path = _SQL_ALCHEMY_MEMORY
        if path != _SQL_ALCHEMY_MEMORY:
            self.__create_parent_dirs(path)
        dsn = "sqlite:///" + path
        self._engine = create_engine(dsn, echo=echo)

    def init_db(self) -> sqlalchemy.engine.Engine:
        if self._db_is_empty():
            Base.metadata.create_all(self._engine)
            self._alembic_stamp_head()
        else:
            if not self._db_is_up_to_date():
                raise Exception(
                    "Database is not up-to-date. Upgrade the database using "
                    "DbInitializer's update_db() method."
                )
        return self._engine

    # TODO: Hide behind module/db.py level function
    def upgrade_db(self) -> None:
        # TODO: Test that this doesn't blow up when in memory
        self._alembic_upgrade()
        self._migrate_results_to_hdf5()

    @staticmethod
    def __create_parent_dirs(path) -> None:
        dirname = os.path.dirname(path)
        if dirname and dirname != "" and dirname != ".":
            os.makedirs(dirname, exist_ok=True)

    def _db_is_empty(self) -> bool:
        cursor = self._engine.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table'"
        )
        return len(cursor.fetchall()) == 0

    def _db_is_up_to_date(self) -> bool:
        script_location = self._abs_path_to("alembic")
        script_ = script.ScriptDirectory(script_location)
        with self._engine.begin() as conn:
            context = migration.MigrationContext.configure(conn)
            db_version = context.get_current_revision()
            latest_version = script_.get_current_head()
            return db_version == latest_version

    @staticmethod
    def _abs_path_to(rel_path: str) -> str:
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        return os.path.join(source_dir, rel_path)

    def _alembic_upgrade(self) -> None:
        alembic_cfg = self._alembic_build_config()
        command.upgrade(alembic_cfg, "head")

    def _alembic_stamp_head(self) -> None:
        alembic_cfg = self._alembic_build_config()
        command.stamp(alembic_cfg, "head")

    def _alembic_build_config(self) -> Config:
        config_location = self._abs_path_to("alembic.ini")
        script_location = self._abs_path_to("alembic")
        alembic_cfg = Config(config_location)
        alembic_cfg.set_main_option("script_location", script_location)
        alembic_cfg.set_main_option("sqlalchemy.url", str(self._engine.url))
        return alembic_cfg

    def _migrate_results_to_hdf5(self):
        logger.debug("Migrating results from sqlite to hdf5")
        results_db = HDF5ResultsDB()
        session_maker = sessionmaker(bind=self._engine)
        with session_maker() as session:
            results = (
                session.query(ResultTable)
                .filter(ResultTable.saved_in_hdf5.is_(False))
                .all()
            )
            if len(results) == 0:
                logger.debug("No results need migrating. Done")
            else:
                logger.debug(f"Found {len(results)} results to migrate")
                migrated_ids = results_db.migrate_result_records(results)
                logger.debug(f"Migrated {len(migrated_ids)} to hdf5")
                for result in results:
                    result.saved_in_hdf5 = True
                session.commit()
                logger.debug("Marked results in sqlite as `saved_in_hdf5`. Done")
