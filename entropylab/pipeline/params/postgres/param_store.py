from __future__ import annotations

import threading
from datetime import timedelta
from pathlib import Path
from typing import Iterator, List, Optional, Dict, Any

import jsonpickle
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from entropylab.pipeline.api.param_store import MergeStrategy, Param
from entropylab.pipeline.api.param_store import ParamStore as ParamStoreABC
from entropylab.pipeline.params.postgres.model import Commit


class ParamStore(ParamStoreABC):
    """Implementation of ParamStore that persists commits to a PostgreSQL database."""

    def __init__(
        self,
        url: Optional[str] | Optional[Path] = None,
        theirs: Optional[Dict | ParamStore] = None,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.THEIRS,
    ):
        super().__init__()

        self.__lock = threading.RLock()
        self.__params: Dict[str, Param] = dict()  # where current params are stored
        self.__tags: Dict[str, List[str]] = dict()  # tags that are mapped to keys
        self.__session_maker = sessionmaker(
            bind=create_engine(
                url,
                json_serializer=jsonpickle.encode,
                json_deserializer=jsonpickle.decode,
            )
        )

        self.checkout()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.__session_maker.close_all()

    """ MutableMapping """

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set self[key] to value. The key-value pair becomes a "param" and
        can be persisted using `commit()` and retrieved later using
        `checkout()`.

        Note: Keys should not start with a dunder (`__`). Such keys are not
        treated as params and are not persisted when `commit()` is called.
        """

        if key.startswith("__") or key.startswith(f"_{self.__class__.__name__}__"):
            # keys that are private attributes are not params and are treated
            # as regular object attributes
            object.__setattr__(self, key, value)
        else:
            with self.__lock:
                self.__params.__setitem__(key, Param(value))
                # self.__is_dirty = True
                # self.__dirty_keys.add(key)

    def __getitem__(self, key: str) -> Any:
        with self.__lock:
            return self.__params.__getitem__(key).value

    def __delitem__(self, *args, **kwargs):
        with self.__lock:
            key = args[0]
            self.__params.__delitem__(*args, **kwargs)
            self.__remove_key_from_tags(key)
            # self.__is_dirty = True
            # self.__dirty_keys.add(key)

    def __remove_key_from_tags(self, key: str):
        for tag in self.__tags:
            if key in self.__tags[tag]:
                self.__tags[tag].remove(key)

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except KeyError:
                raise AttributeError(key)

    def __setattr__(self, key, value):
        try:
            object.__getattribute__(self, key)
        except AttributeError:
            try:
                self[key] = value
            except BaseException:
                raise AttributeError(key)
        else:
            object.__setattr__(self, key, value)

    def __iter__(self) -> Iterator[Any]:
        with self.__lock:
            values = _extract_param_values(self.__params)
            return values.__iter__()

    def __len__(self) -> int:
        with self.__lock:
            return self.__params.__len__()

    def __contains__(self, key):
        with self.__lock:
            return self.__params.__contains__(key)

    def __repr__(self):
        with self.__lock:
            return f"<ParamStore({self.to_dict().__repr__()})>"

    def keys(self):
        with self.__lock:
            return self.__params.keys()

    def to_dict(self) -> Dict:
        with self.__lock:
            return _extract_param_values(self.__params)

    def get_value(self, key: str, commit_id: Optional[str] = None) -> object:
        raise NotImplementedError()

    def get_param(self, key: str, commit_id: Optional[str] = None) -> Param:
        raise NotImplementedError()

    def set_param(self, key: str, value: object, expiration: Optional[timedelta]):
        raise NotImplementedError()

    def rename_key(self, key: str, new_key: str):
        raise NotImplementedError()

    def commit(self, label: Optional[str] = None):
        with self.__lock:
            # if not self.__is_dirty:
            #     return self.__base_commit_id
            # commit_id = self.__generate_commit_id()
            # self.__stamp_dirty_params_with_commit(commit_id, commit_timestamp)
            commit = Commit(
                params=self.__params,
                label=label,
                tags=None,  # TODO: Save self.__tags here
            )
            with self.__session_maker() as session:
                session.add(commit)
                session.commit()
                return commit.id
            # doc = self.__build_document(commit_id, label)
            # with self.__filelock:
            #     doc.doc_id = self.__next_doc_id()
            #     doc_id = self.__db.insert(doc)
            # self.__base_commit_id = doc["metadata"]["id"]
            # self.__base_doc_id = doc_id
            # self.__is_dirty = False
            # self.__dirty_keys.clear()
            # return doc["metadata"]["id"]

    def checkout(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> None:
        commit = self.__get_commit(commit_id, commit_num, move_by)
        if commit:
            self.__checkout(commit)

    def __checkout(self, commit: Commit):
        with self.__lock:
            self.__params.clear()
            self.__params.update(commit.params)
            self.__tags = commit.tags
            # self.__base_commit_id = commit["metadata"]["id"]
            # self.__base_doc_id = commit.doc_id
            # self.__is_dirty = False
            # self.__dirty_keys.clear()

    def list_commits(self, label: Optional[str]):
        raise NotImplementedError()

    def __get_commit(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> Optional[Commit]:
        with self.__lock:
            # if commit_id is not None:
            #     commit = self.__get_commit_by_id(commit_id)
            # elif commit_num is not None:
            #     commit = self.__get_commit_by_num(commit_num)
            # elif move_by is not None:
            #     commit = self.__get_commit_by_move_by(move_by)
            # else:
            #     commit = self.__get_latest_commit()
            #     return commit
            commit = self.__get_latest_commit()
            return commit

    def __get_latest_commit(self) -> Optional[Commit]:
        with self.__lock:
            with self.__session_maker() as session:
                commit = session.query(Commit).order_by(Commit.timestamp.desc()).first()
                return commit

    def list_values(self, key: str) -> pd.DataFrame:
        raise NotImplementedError()

    def merge(
        self,
        theirs: Dict | ParamStore,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    ) -> None:
        raise NotImplementedError()

    def diff(
        self, old_commit_id: Optional[str] = None, new_commit_id: Optional[str] = None
    ) -> Dict[str, Dict]:
        raise NotImplementedError()

    """ Tags """

    def add_tag(self, tag: str, key: str) -> None:
        with self.__lock:
            if key not in self.__params.keys():
                raise KeyError(f"key '{key}' is not in store")
            if tag not in self.__tags:
                self.__tags[tag] = []
            self.__tags[tag].append(key)
            # self.__is_dirty = True

    def remove_tag(self, tag: str, key: str) -> None:
        with self.__lock:
            if tag not in self.__tags:
                return
            if key not in self.__tags[tag]:
                return
            self.__tags[tag].remove(key)
            # self.__is_dirty = True

    def list_keys_for_tag(self, tag: str) -> List[str]:
        with self.__lock:
            if tag not in self.__tags:
                return []
            else:
                return self.__tags[tag]

    def list_tags_for_key(self, key: str):
        tags_for_key = []
        for item in self.__tags.items():
            if key in item[1]:
                tags_for_key.append(item[0])
        return tags_for_key

    """ Temp """

    def save_temp(self) -> None:
        raise NotImplementedError()

    def load_temp(self) -> None:
        raise NotImplementedError()

    @property
    def is_dirty(self):
        return False


""" Static helper methods """


def _map_dict(f, d: Dict) -> Dict:
    values_dict = dict()
    for item in d.items():
        k = item[0]
        v = item[1]
        if not isinstance(v, Param) and isinstance(v, dict):
            values_dict[k] = _map_dict(f, v)
        else:
            values_dict[k] = f(v)
    return values_dict


def _extract_param_values(d: Dict) -> Dict:
    return _map_dict(lambda x: x.value, d)
