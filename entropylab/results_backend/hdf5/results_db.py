import logging
import pickle
from datetime import datetime
from typing import Optional, Any, Iterable

import h5py

from entropylab import RawResultData
from entropylab.api.data_reader import ResultRecord
from entropylab.results_backend.sqlalchemy.model import ResultDataType

HDF_FILENAME = "./entropy.hdf5"


def _experiment_from(dset: h5py.Dataset) -> int:
    return dset.attrs['experiment_id']


def _id_from(dset: h5py.Dataset) -> str:
    return dset.name


def _stage_from(dset: h5py.Dataset) -> int:
    return dset.attrs['stage']


def _label_from(dset: h5py.Dataset) -> str:
    return dset.attrs['label']


def _story_from(dset: h5py.Dataset) -> str:
    return dset.attrs['story']


def _data_from(dset: h5py.Dataset) -> Any:
    if dset.dtype.metadata is not None and dset.dtype.metadata.get('vlen') == str:
        return dset.asstr()[()]
    else:
        if dset.attrs.get('data_type') == ResultDataType.Pickled.value:
            return pickle.loads(dset[()])
        elif dset.attrs.get('data_type') == ResultDataType.String.value:
            return dset[()].decode()
    return dset[()]


def _time_from(dset: h5py.Dataset) -> datetime:
    return datetime.fromisoformat(dset.attrs['time'])


def _build_result_record(dset: h5py.Dataset) -> ResultRecord:
    return ResultRecord(
        experiment_id=_experiment_from(dset),
        # TODO: How to generate a numeric id? Or refactor id to str?
        id=0,  # id_from(dset),
        label=_label_from(dset),
        story=_story_from(dset),
        stage=_stage_from(dset),
        data=_data_from(dset),
        time=_time_from(dset),
        saved_in_hdf5=True  # assumes this method is only called after result is read from HDF5
    )


def _get_all_or_one(group: h5py.Group, name: Optional[str] = None):
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


# noinspection PyMethodMayBeStatic,PyBroadException
class ResultsDB:

    def __init__(self):
        pass

    def save_result(self, experiment_id: int, result: RawResultData) -> str:
        with h5py.File(HDF_FILENAME, 'a') as file:
            path = f"/{experiment_id}/{result.stage}"
            group = file.require_group(path)
            dset = self.__create_dataset(group, result.label, result.data)
            dset.attrs.create('experiment_id', experiment_id)
            dset.attrs.create('stage', result.stage)
            dset.attrs.create('label', result.label or "")
            dset.attrs.create('story', result.story or "")
            dset.attrs.create('time', datetime.now().astimezone().isoformat())
            return dset.name

    def __create_dataset(self, group: h5py.Group, name: str, data: Any) -> h5py.Dataset:
        try:
            dset = group.create_dataset(name=name, data=data)
        except TypeError:
            data_type, pickled = self.__pickle_data(data)
            dtype = h5py.string_dtype(encoding='ascii', length=len(pickled))
            dset = group.create_dataset(name=name, data=pickled, dtype=dtype)
            dset.attrs.create('data_type', data_type.value, dtype='i2')
        return dset

    def __pickle_data(self, data: Any):
        try:
            pickled = pickle.dumps(data)
            data_type = ResultDataType.Pickled
        except Exception:
            pickled = data.__repr__().encode(encoding="UTF-8")
            data_type = ResultDataType.String
        return data_type, pickled

    def migrate_result_records(self, result_records: Iterable[ResultRecord]) -> list[int]:
        if result_records is None:
            return []
        with h5py.File(HDF_FILENAME, 'a') as file:
            sqlalchemy_ids = []
            for result_record in result_records:
                if not result_record.saved_in_hdf5:
                    try:
                        hdf5_id = self.__migrate_result_record(file, result_record)
                        sqlalchemy_ids.append(result_record.id)
                        logging.debug(f"Migrated result with id [{result_record.id}] to HDF5 with id [{hdf5_id}]")
                    except Exception:
                        logging.exception(f"Failed to migrate result with id [{result_record.id}] to HDF5")
            return sqlalchemy_ids

    def __migrate_result_record(self, file: h5py.File, result_record: ResultRecord) -> str:
        path = f"/{result_record.experiment_id}/{result_record.stage}"
        group = file.require_group(path)
        dset = group.create_dataset(
            name=result_record.label,
            data=result_record.data)
        dset.attrs.create('experiment_id', result_record.experiment_id)
        dset.attrs.create('stage', result_record.stage)
        dset.attrs.create('label', result_record.label or "")
        dset.attrs.create('story', result_record.story or "")
        dset.attrs.create('time', result_record.time.astimezone().isoformat())
        dset.attrs.create('migrated_id', result_record.id or "")
        return dset.name

    def get_results(
            self,
            experiment_id: Optional[int] = None,
            stage: Optional[int] = None,
            label: Optional[str] = None,
    ) -> Iterable[ResultRecord]:
        """
        Retrieves ResultRecords from HDF5.
        """
        result = []
        try:
            with h5py.File(HDF_FILENAME, 'r') as file:
                if file is None:
                    return result
                exp_groups = _get_all_or_one(file, experiment_id)
                for exp_group in exp_groups:
                    stage_groups = _get_all_or_one(exp_group, stage)
                    for stage_group in stage_groups:
                        label_dsets = _get_all_or_one(stage_group, label)
                        for label_dset in label_dsets:
                            result.append(_build_result_record(label_dset))
        except FileNotFoundError:
            return result
        return result

    def get_last_result_of_experiment(self, experiment_id: int) -> Optional[ResultRecord]:
        results = list(self.get_results(experiment_id, None, None))
        if results:
            results.sort(key=lambda x: x.time, reverse=True)
            return results[0]
        else:
            return None
