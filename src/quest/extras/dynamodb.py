import os
from .. import BlobStorage, Blob, WorkflowManager, WorkflowFactory, PersistentHistory, History

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    raise ImportError("The 'dynamodb' extra is required to use this module. Run 'pip install quest-py[dynamodb]'.")

class DynamoDBTableCreationException(Exception):
    pass

class DynamoDB:
    def __init__(self):
        self.session = boto3.session.Session(
            os.environ['AWS_ACCESS_KEY_ID'],
            os.environ['AWS_SECRET_ACCESS_KEY'],
            os.environ['AWS_SESSION_TOKEN'],
            os.environ['AWS_REGION']
        )

        self._table_name = 'quest_records'
        self._dynamodb = self.session.resource('dynamodb')
        self._table = self._prepare_table()

    def get_table(self):
        return self._table

    def _prepare_table(self):
        try:
            # Check if table already exists
            table = self._dynamodb.Table(self._table_name)
            table.load()
            return table
        except ClientError as e:
            # If it doesn't, create it
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                table = self._dynamodb.create_table(
                    TableName=self._table_name,
                    KeySchema=[
                        {
                            'AttributeName': 'name',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'key',
                            'KeyType': 'RANGE'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'name',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'key',
                            'AttributeType': 'S'
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    },
                )
                table.meta.client.get_waiter('table_exists').wait(TableName=self._table_name)
                return table
            else:
                raise DynamoDBTableCreationException(f'Error creating DynamoDB table: {e}')

class DynamoDBBlobStorage(BlobStorage):
    def __init__(self, name: str, table):
        self._name = name
        self._table = table

    def write_blob(self, key: str, blob: Blob):
        record = {
            'name': self._name,
            'key': key,
            'blob': blob
        }
        self._table.put_item(Item=record)

    def read_blob(self, key: str) -> Blob:
        primary_key = {
            'name': self._name,
            'key': key
        }
        item = self._table.get_item(Key=primary_key)
        return item.get('blob')

    def has_blob(self, key: str):
        primary_key = {
            'name': self._name,
            'key': key
        }
        item = self._table.get_item(Key=primary_key)
        if item.get('blob'):
            return True
        return False

    def delete_blob(self, key: str):
        primary_key = {
            'name': self._name,
            'key': key
        }
        self._table.delete_item(Key=primary_key)

def create_dynamodb_manager(
        namespace: str,
        factory: WorkflowFactory,
) -> WorkflowManager:
    dynamodb = DynamoDB()

    storage = DynamoDBBlobStorage(namespace, dynamodb.get_table())

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, DynamoDBBlobStorage(wid, dynamodb.get_table()))

    return WorkflowManager(namespace, storage, create_history, factory)