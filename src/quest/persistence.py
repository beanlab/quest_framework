# Enable event histories to be persistent
import json
from hashlib import md5
from pathlib import Path
from typing import Protocol, Union, Dict, Type
from sqlalchemy import create_engine, Column, Integer, String, JSON, select, delete, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, DeclarativeBase, declared_attr
from typing_extensions import TypeVar

from .history import History
from .quest_types import EventRecord

Blob = Union[dict, list, str, int, bool, float]


class BlobStorage(Protocol):
    def write_blob(self, key: str, blob: Blob): ...

    def read_blob(self, key: str) -> Blob: ...

    def has_blob(self, key: str) -> bool: ...

    def delete_blob(self, key: str): ...


class PersistentHistory(History):
    def __init__(self, namespace: str, storage: BlobStorage):
        self._namespace = namespace
        self._storage = storage
        # TODO - use linked list instead of array list
        # master stores head/tail keys
        # each blob is item, prev, next
        # only need to modify blobs for altered nodes
        # on add/delete, and rarely change master head/tail blob
        self._items = []
        self._keys: list[str] = []

        if storage.has_blob(namespace):
            self._keys = storage.read_blob(namespace)
            for key in self._keys:
                self._items.append(storage.read_blob(key))

    def _get_key(self, item: EventRecord) -> str:
        return self._namespace + '.' + md5((item['timestamp'] + item['step_id'] + item['type']).encode()).hexdigest()

    def append(self, item: EventRecord):
        self._items.append(item)
        self._keys.append(key := self._get_key(item))
        self._storage.write_blob(key, item)
        self._storage.write_blob(self._namespace, self._keys)

    def remove(self, item: EventRecord):
        self._items.remove(item)
        self._keys.remove(key := self._get_key(item))
        self._storage.delete_blob(key)
        self._storage.write_blob(self._namespace, self._keys)

    def __iter__(self):
        return iter(self._items)

    def __reversed__(self):
        return reversed(self._items)


class LocalFileSystemBlobStorage(BlobStorage):
    def __init__(self, root_folder: Path):
        root_folder.mkdir(parents=True, exist_ok=True)
        self._root = root_folder

    def _get_file(self, key: str) -> Path:
        return self._root / (key + '.json')

    def write_blob(self, key: str, blob: Blob):
        self._get_file(key).write_text(json.dumps(blob))

    def read_blob(self, key: str) -> Blob:
        return json.loads(self._get_file(key).read_text())

    def has_blob(self, key: str) -> bool:
        return self._get_file(key).exists()

    def delete_blob(self, key: str):
        self._get_file(key).unlink()

Base = declarative_base()
metadata = MetaData()

T = TypeVar('T', bound='BaseRecordModel')

class BaseRecordModel(Base):
    __abstract__ = True

    key = Column(String, primary_key=True)
    blob = Column(JSON)

    def __init__(self,
                 key: str,
                 blob: Blob):
        self.id = key
        self.blob = json.dumps(blob) # TODO should I do this here or in the blob storage?

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.id}>'

class SqlBlobStorage(BlobStorage):
    def __init__(self, workflow_name, record_model: BaseRecordModel, db):
        self._workflow_name = workflow_name
        self.record_model = record_model
        self._db = db

    def write_blob(self, key: str, blob: Blob):
        with self._db.get_session() as session:
            new_record = self.record_model.__init__(key, blob) # TODO Do I need to directly call dunder here?
            session.add(new_record)
            session.commit()

    def read_blob(self, key: str) -> Blob:
        with self._db.get_session() as session:
            query = session.query(self.record_model)
            query = query.filter(getattr(self.record_model, 'key') == key)
            return query.all() # TODO This should just return the blob

    def has_blob(self, key: str) -> bool: ...

    def delete_blob(self, key: str): ...

class Database: # TODO: This should just be a basic Database class that creates a connection on a URL

    def __init__(self, db_url: str):
        self._db_url = db_url
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.workflow_tables: Dict[str, Type[Base]] = {}

    def create_workflow_table(self, workflow_name) -> Type[Base]:
        if workflow_name in self.workflow_tables:
            return self.workflow_tables[workflow_name]

        workflow_table_attrs = {
            '__tablename__': workflow_name,
            'id': Column(String, primary_key=True),
            'blob': Column(JSON) # TODO: Expand this into the separate columns desired
        }

        workflow_table = type(
            workflow_name,
            (Base,),
            workflow_table_attrs
        )

        self.workflow_tables[workflow_name] = workflow_table

        Base.metadata.create_all(self.engine)

# TODO
# I want PersistentHistory to have an instance of the SqlBlobStorage that is connected to the database and is 1:1 with
#  a table so I can add, remove, etc. on that table for one workflow. In order to do this, there needs to be
#  a Database (in the workflow manager) that creates the tables and passes them into persistent history as BlobStorage
#  objects.


class InMemoryBlobStorage(BlobStorage):
    def __init__(self):
        self._data = {}

    def write_blob(self, key: str, blob: Blob):
        self._data[key] = blob

    def read_blob(self, key: str) -> Blob:
        return self._data[key]

    def has_blob(self, key: str) -> bool:
        return key in self._data

    def delete_blob(self, key: str):
        del self._data[key]
