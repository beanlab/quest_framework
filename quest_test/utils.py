import asyncio
import sys
from functools import wraps

from quest import WorkflowManager
from quest.persistence import InMemoryBlobStorage, PersistentHistory


def timeout(delay):
    if 'pydevd' in sys.modules:  # i.e. debug mode
        # Return a no-op decorator
        return lambda func: func

    def decorator(func):
        @wraps(func)
        async def new_func(*args, **kwargs):
            async with asyncio.timeout(delay):
                return await func(*args, **kwargs)

        return new_func

    return decorator


def create_in_memory_workflow_manager(workflows: dict):
    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    def create_workflow(wtype: str):
        return workflows[wtype]

    return WorkflowManager('test', storage, create_history, create_workflow)

from moto import mock_dynamodb
import boto3

@mock_aws
def mock_aws_session():
    return boto3.session.Session()

# from moto import mock_dynamodb
# import boto3
#
# def mock_dynamodb(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         @mock_aws
#         def create_mock_table:
#             # Set up the mock environment
#             dynamodb = boto3.resource('dynamodb')
#             # Create a mock table (this can be adjusted as needed)
#             dynamodb.create_table(
#                 TableName='mock_table',
#                 KeySchema=[
#                     {
#                         'AttributeName': 'id',
#                         'KeyType': 'HASH'  # Partition key
#                     }
#                 ],
#                 AttributeDefinitions=[
#                     {
#                         'AttributeName': 'id',
#                         'AttributeType': 'S'  # String type
#                     }
#                 ],
#                 ProvisionedThroughput={
#                     'ReadCapacityUnits': 1,
#                     'WriteCapacityUnits': 1
#                 }
#             )
#         return func(*args, **kwargs)