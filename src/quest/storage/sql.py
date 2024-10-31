from sqlalchemy import create_engine, Column, Integer, String, JSON, Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from .blob import BlobStorage, Blob

Base = declarative_base()

class _RecordModel(Base):
    __tablename__ = 'records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String) # TODO good name for this?
    key = Column(String)
    blob = Column(JSON)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'

class SQLDatabase:

    def __init__(self, db_url: str):
        self._db_url = db_url
        self._engine = create_engine(db_url)

        Base.metadata.create_all(self._engine)

    def get_engine(self) -> Engine:
        return self._engine

class SqlBlobStorage(BlobStorage):
    def __init__(self, name, engine):
        self._name = name
        self._engine = engine

    def _get_session(self):
        return sessionmaker(bind=self._engine)()

    def write_blob(self, key: str, blob: Blob):
        # Check to see if a blob exists, if so rewrite it
        with self._get_session() as session:
            records = session.query(_RecordModel).filter(_RecordModel.name == self._name).all()
            record_to_update = next((record for record in records if record.key == key), None)
            if record_to_update:
                record_to_update.blob = blob
            else:
                new_record = _RecordModel(name=self._name, key=key, blob=blob)
                session.add(new_record)
            session.commit()

    # noinspection PyTypeChecker
    def read_blob(self, key: str) -> Blob | None:
        with self._get_session() as session:
            records = session.query(_RecordModel).filter(_RecordModel.name == self._name).all()
            for record in records:
                if record.key == key:
                    return record.blob

    def has_blob(self, key: str) -> bool:
        with self._get_session() as session:
            records = session.query(_RecordModel).filter(_RecordModel.name == self._name).all()
            for record in records:
                if record.key == key:
                    return True
            return False

    def delete_blob(self, key: str):
        with self._get_session() as session:
            records = session.query(_RecordModel).filter(_RecordModel.name == self._name).all()
            for record in records:
                if record.key == key:
                    session.delete(record)
                    session.commit()