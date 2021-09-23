import os
from pathlib import Path
from typing import TypeVar, Type, Tuple

from alembic import script, command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy import create_engine
import sqlalchemy.engine
from sqlalchemy.orm import sessionmaker

from entropylab.logger import logger
from entropylab.results_backend.sqlalchemy.model import Base, ResultTable, MetadataTable
from entropylab.results_backend.sqlalchemy.project import project_name, project_path
from entropylab.results_backend.sqlalchemy.storage import HDF5Storage, EntityType

_SQL_ALCHEMY_MEMORY = ":memory:"

T = TypeVar("T", bound=Base)

_ENTROPY_DIRNAME = ".entropy"
_DB_FILENAME = "entropy.db"
_HDF5_FILENAME = "entropy.hdf5"


class _DbInitializer:
    def __init__(self, path: str, echo=False):
        """
        :param path: path to directory containing Entropy project
        :param echo: if True, the database engine will log all statements
        """
        if path is not None and Path(path).suffix == ".db":
            logger.error(
                "_DbInitializer provided with path to a sqlite database. This is deprecated."
            )
            raise RuntimeError(
                "Providing the SqlAlchemyDB() constructor with a path to a sqlite "
                "database is deprecated. You should instead provide the path to a "
                "directory containing an Entropy project.\n"
                "To upgrade your existing sqlite database file to an Entropy project "
                "please use the entropylab.results_backend.sqlalchemy.upgrade_db() "
                "function.\n"
                "* Before upgrading be sure to back up your database to a safe place *."
            )
        if path is not None and os.path.isfile(path):
            logger.error(
                "_DbInitializer provided with path to a file, not a directory."
            )
            raise RuntimeError(
                "SqlAlchemyDB() constructor provided with a path to a file but "
                "expects the path to an Entropy project folder"
            )
        if path is None or path == _SQL_ALCHEMY_MEMORY:
            logger.debug("_DbInitializer is in in-memory mode")
            self._storage = HDF5Storage()
            self._engine = create_engine("sqlite:///" + _SQL_ALCHEMY_MEMORY, echo=echo)
            self._alembic_util = _AlembicUtil(self._engine)
        else:
            logger.debug("_DbInitializer is in project directory mode")
            creating_new = os.path.isdir(path)
            entropy_dir_path = os.path.join(path, _ENTROPY_DIRNAME)
            os.makedirs(entropy_dir_path, exist_ok=True)
            logger.debug(f"Entropy directory is at: {entropy_dir_path}")

            db_file_path = os.path.join(entropy_dir_path, _DB_FILENAME)
            logger.debug(f"DB file is at: {db_file_path}")

            hdf5_file_path = os.path.join(entropy_dir_path, _HDF5_FILENAME)
            logger.debug(f"hdf5 file is at: {hdf5_file_path}")

            self._engine = create_engine("sqlite:///" + db_file_path, echo=echo)
            self._storage = HDF5Storage(hdf5_file_path)
            self._alembic_util = _AlembicUtil(self._engine)
            if creating_new:
                self.print_project_created(path)

    @staticmethod
    def print_project_created(path):
        print(
            f"New Entropy project '{project_name(path)}' created at '{project_path(path)}'"
        )

    def init_db(self) -> Tuple[sqlalchemy.engine.Engine, HDF5Storage]:
        if self._db_is_empty():
            Base.metadata.create_all(self._engine)
            self._alembic_util.stamp_head()
        else:
            if not self._alembic_util.db_is_up_to_date():
                path = str(self._engine.url)
                raise RuntimeError(
                    f"The database at {path} is not up-to-date. Update the database "
                    f"using the entropylab.results_backend.sqlalchemy.upgrade_db() function. "
                    f"* Before upgrading be sure to back up your database to a safe place *."
                )
        return self._engine, self._storage

    def upgrade_db(self):
        upgrader = _DbUpgrader(self._engine, self._storage)
        upgrader.upgrade_db()

    def _db_is_empty(self) -> bool:
        cursor = self._engine.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table'"
        )
        return len(cursor.fetchall()) == 0


class _DbUpgrader:
    def __init__(self, engine: sqlalchemy.engine.Engine, storage: HDF5Storage):
        self._engine = engine
        self._storage = storage
        self._alembic_util = _AlembicUtil(self._engine)

    def upgrade_db(self) -> None:  # TODO: path as argument
        self._convert_to_project()
        self._alembic_util.upgrade()
        self._migrate_results_to_hdf5()
        self._migrate_metadata_to_hdf5()

    def _convert_to_project(self):
        pass  # TODO: !!!

    def _migrate_results_to_hdf5(self) -> None:
        self._migrate_rows_to_hdf5(EntityType.RESULT, ResultTable)

    def _migrate_metadata_to_hdf5(self) -> None:
        self._migrate_rows_to_hdf5(EntityType.METADATA, MetadataTable)

    def _migrate_rows_to_hdf5(self, entity_type: EntityType, table: Type[T]):
        logger.debug(f"Migrating {entity_type.name} rows from sqlite to hdf5")
        session_maker = sessionmaker(bind=self._engine)
        with session_maker() as session:
            rows = session.query(table).filter(table.saved_in_hdf5.is_(False)).all()
            if len(rows) == 0:
                logger.debug(f"No {entity_type.name} rows need migrating. Done")
            else:
                logger.debug(f"Found {len(rows)} {entity_type.name} rows to migrate")
                self._storage.migrate_rows(entity_type, rows)
                logger.debug(f"Migrated {len(rows)} {entity_type.name} to hdf5")
                for row in rows:
                    row.saved_in_hdf5 = True
                session.commit()
                logger.debug(
                    f"Marked all {entity_type.name} rows in sqlite as `saved_in_hdf5`. Done"
                )


class _AlembicUtil:
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self._engine = engine

    @staticmethod
    def _abs_path_to(rel_path: str) -> str:
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        return os.path.join(source_dir, rel_path)

    def upgrade(self) -> None:
        with self._engine.connect() as connection:
            alembic_cfg = self._alembic_build_config(connection)
            command.upgrade(alembic_cfg, "head")

    def stamp_head(self) -> None:
        with self._engine.connect() as connection:
            alembic_cfg = self._alembic_build_config(connection)
            command.stamp(alembic_cfg, "head")

    def db_is_up_to_date(self) -> bool:
        script_location = self._abs_path_to("alembic")
        script_ = script.ScriptDirectory(script_location)
        with self._engine.begin() as conn:
            context = migration.MigrationContext.configure(conn)
            db_version = context.get_current_revision()
            latest_version = script_.get_current_head()
            return db_version == latest_version

    def _alembic_build_config(self, connection: sqlalchemy.engine.Connection) -> Config:
        config_location = self._abs_path_to("alembic.ini")
        script_location = self._abs_path_to("alembic")
        alembic_cfg = Config(config_location)
        alembic_cfg.set_main_option("script_location", script_location)
        """
        Modified by @urig to share a single engine across multiple (typically in-memory)
        connections, based on this cookbook recipe:
        https://alembic.sqlalchemy.org/en/latest/cookbook.html#connection-sharing
        """
        alembic_cfg.set_main_option("sqlalchemy.url", "sqlite:///:memory:")
        alembic_cfg.attributes["connection"] = connection  # overrides dummy url above
        return alembic_cfg
