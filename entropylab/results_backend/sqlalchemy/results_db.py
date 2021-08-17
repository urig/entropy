import pickle
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Iterable, TypeVar, Callable

import h5py

from entropylab import RawResultData
from entropylab.api.data_reader import ResultRecord, MetadataRecord
from entropylab.api.data_writer import Metadata
from entropylab.logger import logger
from entropylab.results_backend.sqlalchemy.model import ResultDataType, ResultTable

# TODO: Inject via __init__() (to be read from config above)
HDF_FILENAME = "./entropy.hdf5"

T = TypeVar("T")


def _experiment_from(dset: h5py.Dataset) -> int:
    return dset.attrs["experiment_id"]


def _id_from(dset: h5py.Dataset) -> str:
    return dset.name


def _stage_from(dset: h5py.Dataset) -> int:
    return dset.attrs["stage"]


def _label_from(dset: h5py.Dataset) -> str:
    return dset.attrs["label"]


def _story_from(dset: h5py.Dataset) -> str:
    if "story" in dset.attrs:
        return dset.attrs["story"]
    return ""


def _data_from(dset: h5py.Dataset) -> Any:
    data = dset[()]
    if dset.dtype.metadata and dset.dtype.metadata.get("vlen") == str:
        return dset.asstr()[()]
    elif dset.attrs.get("data_type") == ResultDataType.Pickled.value:
        return pickle.loads(data)
    elif dset.attrs.get("data_type") == ResultDataType.String.value:
        return data.decode()
    else:
        return data


def _time_from(dset: h5py.Dataset) -> datetime:
    return datetime.fromisoformat(dset.attrs["time"])


def _build_result_record(dset: h5py.Dataset) -> ResultRecord:
    return ResultRecord(
        experiment_id=_experiment_from(dset),
        # TODO: Get confirmation to change id to string
        id=0,  # id_from(dset),
        label=_label_from(dset),
        story=_story_from(dset),
        stage=_stage_from(dset),
        data=_data_from(dset),
        time=_time_from(dset),
    )


def _build_metadata_record(dset: h5py.Dataset) -> Metadata:
    return MetadataRecord(
        experiment_id=_experiment_from(dset),
        # TODO: Get confirmation to change id to string
        id=0,  # id_from(dset),
        label=_label_from(dset),
        stage=_stage_from(dset),
        data=_data_from(dset),
        time=_time_from(dset),
    )


def _get_all_or_single(group: h5py.Group, name: Optional[str] = None):
    """
    Returns all or one child from an h5py.Group

    Parameters
    ----------
    group group to get child or children from. Can be h5py.File itself.
    name name of child to get. If None, indicates all children should be retrieved.

    Returns
    -------
    A list of group children (either h5py.Group or h5py.Datasets)
    """
    if name is None:
        return list(group.values())
    else:
        if str(name) in group:
            return [group[str(name)]]
        else:
            return []


class EntityType(Enum):
    RESULT = 1
    METADATA = 2


# noinspection PyMethodMayBeStatic,PyBroadException
class HDF5ResultsDB:
    def __init__(self):
        self._check_file_permissions()

    def _check_file_permissions(self):
        file = h5py.File(HDF_FILENAME, "a")
        file.close()

    def save_result(self, experiment_id: int, result: RawResultData) -> str:
        with h5py.File(HDF_FILENAME, "a") as file:
            return self._save_entity_to_file(
                file,
                EntityType.RESULT,
                experiment_id,
                result.stage,
                result.label,
                result.data,
                datetime.now(),
                result.story,
            )

    def save_metadata(self, experiment_id: int, metadata: Metadata):
        with h5py.File(HDF_FILENAME, "a") as file:
            return self._save_entity_to_file(
                file,
                EntityType.METADATA,
                experiment_id,
                metadata.stage,
                metadata.label,
                metadata.data,
                datetime.now(),
            )

    def _save_entity_to_file(
        self,
        file: h5py.File,
        entity_type: EntityType,
        experiment_id: int,
        stage: int,
        label: str,
        data: Any,
        time: datetime,
        story: Optional[str] = None,
        migrated_id: Optional[int] = None,
    ) -> str:
        path = f"/experiments/{experiment_id}/{stage}/{label}"
        group = file.require_group(path)
        dset = self._create_dataset(group, entity_type, data)
        dset.attrs.create("experiment_id", experiment_id)
        dset.attrs.create("stage", stage)
        dset.attrs.create("label", label)
        dset.attrs.create("time", time.astimezone().isoformat())
        if story:
            dset.attrs.create("story", story or "")
        if migrated_id:
            dset.attrs.create("migrated_id", migrated_id or "")
        return dset.name

    def _create_dataset(
        self, group: h5py.Group, entity_type: EntityType, data: Any
    ) -> h5py.Dataset:
        name = entity_type.name.lower()
        try:
            dset = group.create_dataset(name=name, data=data)
        except TypeError:
            data_type, pickled = self.__pickle_data(data)
            # TODO: Why ascii? Why string_dtype? Maybe just save bytes...
            dtype = h5py.string_dtype(encoding="ascii", length=len(pickled))
            dset = group.create_dataset(name=name, data=pickled, dtype=dtype)
            dset.attrs.create("data_type", data_type.value, dtype="i2")
        return dset

    def __pickle_data(self, data: Any):
        try:
            pickled = pickle.dumps(data)
            data_type = ResultDataType.Pickled
        except Exception:
            pickled = data.__repr__().encode(encoding="UTF-8")
            data_type = ResultDataType.String
        return data_type, pickled

    def migrate_result_records(self, results: Iterable[ResultTable]) -> list[int]:
        # TODO: Add docstring
        if results is None:
            return []
        with h5py.File(HDF_FILENAME, "a") as file:
            sqlalchemy_ids = []
            for result in results:
                if not result.saved_in_hdf5:
                    try:
                        result_record = result.to_record()
                        hdf5_id = self._migrate_result_record(file, result_record)
                        sqlalchemy_ids.append(result.id)
                        logger.debug(
                            f"Migrated result with id [{result.id}] to HDF5 with id [{hdf5_id}]"
                        )
                    except Exception:
                        logger.exception(
                            f"Failed to migrate result with id [{result.id}] to HDF5"
                        )
            return sqlalchemy_ids

    def _migrate_result_record(
        self, file: h5py.File, result_record: ResultRecord
    ) -> str:
        return self._save_entity_to_file(
            file,
            EntityType.RESULT,
            result_record.experiment_id,
            result_record.stage,
            result_record.label,
            result_record.data,
            result_record.story,
            result_record.time,
            result_record.id,
        )

    def _get_experiment_entities(
        self,
        entity_type: EntityType,
        convert_from_dset: Callable,
        experiment_id: Optional[int] = None,
        stage: Optional[int] = None,
        label: Optional[str] = None,
    ) -> Iterable[T]:
        dsets = []
        try:
            with h5py.File(HDF_FILENAME, "r") as file:
                top_group = file["experiments"]
                exp_groups = _get_all_or_single(top_group, experiment_id)
                for exp_group in exp_groups:
                    stage_groups = _get_all_or_single(exp_group, stage)
                    for stage_group in stage_groups:
                        label_groups = _get_all_or_single(stage_group, label)
                        for label_group in label_groups:
                            dset_name = entity_type.name.lower()
                            dset = label_group[dset_name]
                            dsets.append(convert_from_dset(dset))
            return dsets
        except FileNotFoundError:
            # TODO: Log input args:
            logger.exception("FileNotFoundError in get_experiment_entities()")
            return dsets

    def get_results(
        self,
        experiment_id: Optional[int] = None,
        stage: Optional[int] = None,
        label: Optional[str] = None,
    ) -> Iterable[ResultRecord]:
        entities = self._get_experiment_entities(
            EntityType.RESULT, _build_result_record, experiment_id, stage, label
        )
        return entities

    def get_metadata_records(
        self,
        experiment_id: Optional[int] = None,
        stage: Optional[int] = None,
        label: Optional[str] = None,
    ) -> Iterable[MetadataRecord]:
        entities = self._get_experiment_entities(
            EntityType.METADATA, _build_metadata_record, experiment_id, stage, label
        )
        return entities
        pass

    def get_last_result_of_experiment(
        self, experiment_id: int
    ) -> Optional[ResultRecord]:
        results = list(self.get_results(experiment_id, None, None))
        if results and len(results) > 0:
            results.sort(key=lambda x: x.time, reverse=True)
            return results[0]
        else:
            return None
