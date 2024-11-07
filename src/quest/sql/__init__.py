from .sql import SQLDatabase, SqlBlobStorage

from quest import WorkflowFactory, WorkflowManager, PersistentHistory, History

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