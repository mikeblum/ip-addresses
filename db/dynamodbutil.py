import decimal
import logging
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

DYNAMODB_TABLE_NAME = 'db.ipbot.io'
TIMESTAMP_FORMAT = '%Y-%m-%dT%l:%M:%S%z'

# logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# silence boto3 logging - defaults to noisy debug
logging.getLogger('boto3').setLevel(logging.WARNING) 
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('nose').setLevel(logging.WARNING)

# dynamoDB table must be in the same region as the Lambda function
client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# deserializer
dynamo_deserializer = TypeDeserializer()

def setup_dynamo_db_table():
    try:
        client.describe_table(TableName=DYNAMODB_TABLE_NAME)
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        logger.info("Table status: %s" % table.table_status)
        return table
    except Exception as e:
        if "ResourceNotFoundException" in str(e):
            logger.info("Table does not exist: %s" % DYNAMODB_TABLE_NAME)
            logger.info("Creating table: %s" % DYNAMODB_TABLE_NAME)
            table = dynamodb.create_table(
                TableName=DYNAMODB_TABLE_NAME,
                KeySchema=[
                    {
                        'AttributeName': 'cidr',
                        'KeyType': 'HASH'  #Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'cidr',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 100,
                    'WriteCapacityUnits': 200
                }
            )
            return table
        else:
            logger.info(str(e))
            return None

def drop_table():
    client.describe_table(TableName=DYNAMODB_TABLE_NAME)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    table.delete()

def merge_records(records):
    '''
    Merge together multiple SqlAchemy records into a single dictionary
    '''
    result = {}
    for record in records:
        # convert a SqlAlchemy record to a Python dict
        dictionary = record.as_dict()
        result.update(dictionary)
    return result
    
def convert_to_dynamodb_record(record):
    '''
    record - a Python dict object
    Resolve any type support issues in DynamoDB
    - convert floats to Decimal format
    '''
    is_empty_str = lambda x: x == "" 

    converted_record = {}
    for key in record:
        if isinstance(record[key], float):
            # dynamoDB doesn't support floats. Store as a String with 3 places precision
            converted_record[key] = '%.3f' % record[key]
        elif is_empty_str(record[key]):
            # dynamoDB doesn't support empty strings
            converted_record[key] = None
        else:
            converted_record[key] = record[key]
    return converted_record

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)