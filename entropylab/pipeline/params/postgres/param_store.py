from __future__ import annotations

import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Iterator, List, Optional, Dict, Any

import pandas as pd

from entropylab.pipeline.api.param_store import MergeStrategy, Param
from entropylab.pipeline.api.param_store import ParamStore as ParamStoreABC
from entropylab.pipeline.params.postgres.model import Commit


class ParamStore(ParamStoreABC):
    """Implementation of ParamStore that persists commits to a PostgreSQL database."""

    def __init__(
        self,
        path: Optional[str] | Optional[Path] = None,
        theirs: Optional[Dict | ParamStore] = None,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.THEIRS,
    ):
        super().__init__()

        self.__lock = threading.RLock()
        self.__params: Dict[str, Param] = dict()  # where current params are stored

    def __setitem__(self, key: str, value: Any) -> None:
        with self.__lock:
            self.__params.__setitem__(key, Param(value))

    def __delitem__(self, *args, **kwargs):
        raise NotImplementedError()

    def __getitem__(self, key: str) -> Any:
        with self.__lock:
            return self.__params.__getitem__(key).value

    def __len__(self) -> int:
        with self.__lock:
            return self.__params.__len__()

    def __iter__(self) -> Iterator[Any]:
        raise NotImplementedError()

    def keys(self):
        raise NotImplementedError()

    def to_dict(self):
        raise NotImplementedError()

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
            commit_timestamp = time.time_ns()  # nanoseconds since epoch
            commit = Commit(timestamp=time.time_ns())
            # self.__stamp_dirty_params_with_commit(commit_id, commit_timestamp)
            # doc = self.__build_document(commit_id, label)
            # with self.__filelock:
            #     doc.doc_id = self.__next_doc_id()
            #     doc_id = self.__db.insert(doc)
            # self.__base_commit_id = doc["metadata"]["id"]
            # self.__base_doc_id = doc_id
            # self.__is_dirty = False
            # self.__dirty_keys.clear()
            # return doc["metadata"]["id"]

    def checkout(self, commit_id: str, commit_num: int = None, move_by: int = None):
        raise NotImplementedError()

    def list_commits(self, label: Optional[str]):
        raise NotImplementedError()

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

    def add_tag(self, tag: str, key: str) -> None:
        raise NotImplementedError()

    def remove_tag(self, tag: str, key: str) -> None:
        raise NotImplementedError()

    def list_keys_for_tag(self, tag: str) -> List[str]:
        raise NotImplementedError()

    def list_tags_for_key(self, key: str):
        raise NotImplementedError()

    def save_temp(self) -> None:
        raise NotImplementedError()

    def load_temp(self) -> None:
        raise NotImplementedError()

    @property
    def is_dirty(self):
        raise NotImplementedError()
