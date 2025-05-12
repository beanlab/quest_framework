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
    name = Column(String)  # TODO good name for this?
    key = Column(String)
    blob = Column(JSON)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'


class SQLDatabase:

    def __init__(self, db_url: str):
        self._db_url = db_url
        self._engine = create_engine(db_url)

    def get_session(self) -> Session:
        return sessionmaker(bind=self._engine)()


class SqlBlobStorage(BlobStorage):
    def __init__(self, name, session: Session):
        self._name = name
        self._session = session

        Base.metadata.create_all(self._session.connection())

    def _get_session(self):
        return self._session

    def write_blob(self, key: str, blob: Blob):
        # Check to see if a blob exists, if so rewrite it
        record_to_update = self._get_session().query(RecordModel).filter(RecordModel.name == self._name).one_or_none()
        if record_to_update:
            record_to_update.blob = blob
        else:
            new_record = RecordModel(name=self._name, key=key, blob=blob)
            self._get_session().add(new_record)
        self._get_session().commit()

    # noinspection PyTypeChecker
    def read_blob(self, key: str) -> Blob | None:
        records = self._get_session().query(RecordModel).filter(RecordModel.name == self._name).all()
        for record in records:
            if record.key == key:
                return record.blob

    def has_blob(self, key: str) -> bool:
        records = self._get_session().query(RecordModel).filter(RecordModel.name == self._name).all()
        for record in records:
            if record.key == key:
                return True
        return False

    def delete_blob(self, key: str):
        records = self._get_session().query(RecordModel).filter(RecordModel.name == self._name).all()
        for record in records:
            if record.key == key:
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
