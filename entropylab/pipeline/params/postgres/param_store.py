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
        return False
