from .. import WorkflowFactory, WorkflowManager, PersistentHistory, History, BlobStorage, Blob

try:
    from sqlalchemy import create_engine, Column, Integer, String, JSON, Engine
    from sqlalchemy.orm import sessionmaker, Session, declarative_base
except ImportError:
    raise ImportError("The 'sql' extra is required to use this module. Run 'pip install quest-py[sql]'.")

Base = declarative_base()


class RecordModel(Base):
    __tablename__ = 'records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    wid = Column(String)  # TODO good name for this?
    key = Column(String)
    blob = Column(JSON)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.wid}>'


class SQLDatabase:

    def __init__(self, db_url: str):
        self._db_url = db_url
        self._engine = create_engine(db_url)

        Base.metadata.create_all(self._engine)

    def get_session(self) -> Session:
        return sessionmaker(bind=self._engine)()


class SqlBlobStorage(BlobStorage):
    def __init__(self, wid, session):
        self._wid = wid
        self._session = session

    def _get_session(self):
        return self._session

    def write_blob(self, key: str, blob: Blob):
        # Check to see if a blob exists, if so rewrite it
        record_to_update = self._get_session().query(RecordModel).filter(RecordModel.wid == self._wid).one_or_none()
        if record_to_update:
            record_to_update.blob = blob
        else:
            new_record = RecordModel(wid=self._wid, key=key, blob=blob)
            self._get_session().add(new_record)
        self._get_session().commit()

    # noinspection PyTypeChecker
    def read_blob(self, key: str) -> Blob | None:
        records = self._get_session().query(RecordModel).filter(RecordModel.wid == self._wid).all()
        for record in records:
            if record.key == key:
                return record.blob

    def has_blob(self, key: str) -> bool:
        records = self._get_session().query(RecordModel).filter(RecordModel.wid == self._wid).all()
        for record in records:
            if record.key == key:
                return True
        return False

    def delete_blob(self, key: str):
        records = self._get_session().query(RecordModel).filter(RecordModel.wid == self._wid).all()
        for record in records:
            if record.key == key:
                self._get_session().delete(record)
                self._get_session().commit()

    def delete_storage(self):
        records = self._get_session().query(RecordModel).filter(RecordModel.wid == self._wid).all()
        for record in records:
            self._get_session().delete(record)
            self._get_session().commit()


def create_sql_manager(
        db_url: str,
        namespace: str,
        factory: WorkflowFactory
) -> WorkflowManager:
    database = SQLDatabase(db_url)

    storage = SqlBlobStorage(namespace, database.get_session())

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, SqlBlobStorage(wid, database.get_session()))

    return WorkflowManager(namespace, storage, create_history, factory)
