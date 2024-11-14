import os
import json

from .. import BlobStorage, Blob, WorkflowManager, WorkflowFactory, PersistentHistory, History

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    raise ImportError("The 's3' extra is required to use this module. Run 'pip install quest-py[s3]'.")


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
