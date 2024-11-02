import asyncio

async def ainput(*args):
    return await asyncio.to_thread(input, *args)

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