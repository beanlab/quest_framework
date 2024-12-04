import os
import json

from .. import BlobStorage, Blob, WorkflowManager, WorkflowFactory, PersistentHistory, History

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    raise ImportError("The 'aws' extra is required to use this module. Run 'pip install quest-py[aws]'.")


class S3Bucket:
    def __init__(self):
        self._region = os.environ['AWS_REGION']
        self.session = boto3.session.Session(
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            aws_session_token=os.environ['AWS_SESSION_TOKEN'],
            region_name=self._region
        )

        self._bucket_name = 'quest-records'
        self._s3_client = self.session.client('s3')

        self._prepare_bucket()

    def get_s3_client(self):
        return self._s3_client

    def get_bucket_name(self):
        return self._bucket_name

    def _prepare_bucket(self):
        try:
            self._s3_client.head_bucket(Bucket=self._bucket_name)
        except ClientError:
            if self._region:
                self._s3_client.create_bucket(
                    Bucket=self._bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self._region}
                )
            else:
                self._s3_client.create_bucket(Bucket=self._bucket_name)


class S3BlobStorage(BlobStorage):
    def __init__(self, name, s3_client, bucket_name):
        self._bucket_name = bucket_name
        self._name = name
        self._s3_client = s3_client

    def write_blob(self, key: str, blob: Blob):
        object_key = f"{self._name}/{key}"

        json_blob = json.dumps(blob)

        self._s3_client.put_object(
            Bucket=self._bucket_name,
            Key=object_key,
            Body=json_blob,
            ContentType='application/json'
        )

    def read_blob(self, key: str) -> Blob:
        object_key = f"{self._name}/{key}"
        item = self._s3_client.get_object(Bucket=self._bucket_name, Key=object_key)
        blob = json.loads(item['Body'].read().decode('utf-8'))
        return blob

    def has_blob(self, key: str) -> bool:
        object_key = f"{self._name}/{key}"
        try:
            self._s3_client.head_object(Bucket=self._bucket_name, Key=object_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    def delete_blob(self, key: str):
        object_key = f"{self._name}/{key}"
        self._s3_client.delete_object(Bucket=self._bucket_name, Key=object_key)


def create_s3_manager(
        namespace: str,
        factory: WorkflowFactory,
) -> WorkflowManager:
    s3 = S3Bucket()

    storage = S3BlobStorage(namespace, s3.get_s3_client(), s3.get_bucket_name())

    def create_history(wid: str) -> History:
        return PersistentHistory(wid, S3BlobStorage(wid, s3.get_s3_client(), s3.get_bucket_name()))

    return WorkflowManager(namespace, storage, create_history, factory)


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


class DynamoDBTableCreationException(Exception):
    pass


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
