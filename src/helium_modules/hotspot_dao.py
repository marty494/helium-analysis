import boto3
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client('dynamodb', region_name='us-east-2')
dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-2', endpoint_url="https://dynamodb.us-east-2.amazonaws.com")

#
# FETCH THE TABLE FROM DYNAMODB
# IF THE TABLE DOES NOT EXIST THEN THIS METHOD WILL CREATE THE TABLE
#
def get_table(tablename):
    logger.debug('get_table: ' + tablename)
    existing_tables = list(dynamodb_resource.tables.all())

    for table in existing_tables:
        if table.name == tablename:
            return table

    logger.info('create_table: ' + tablename)
    table = dynamodb_client.create_table(
      TableName=tablename,
        AttributeDefinitions=[
            {
                'AttributeName': 'address',
                'AttributeType': 'S'
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'address',
                'KeyType': 'HASH'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1,
        },
    )

    waiter = dynamodb_client.get_waiter('table_exists')
    waiter.wait(TableName=tablename)

    return table

#
# RETREIVE THE HOTSPOT CONFIG DATA FROM DYNAMODB
# EMPTY DEFAULT VALUES ARE RETURNED FOR THE SUPPLIED HOTSPOT IF MISSING
#
def get_hotspot_details(table, hotspot_address):
    response = table.query(
      KeyConditionExpression=Key('address').eq(hotspot_address)
    )

    if 'Count' in response:
        if response['Count'] > 0:
            record = response['Items'][0]
            return record

    hotspot_details = {
        'name': '',
        'born_date': '',
        'latest_processed_date': '',
        'earliest_processed_date': '',
        'activity_count': ''
    }

    return hotspot_details

#
# CREATE A NEW ENTRY FOR THE SUPPLIED HOTSPOT
# ONLY USED WHEN A NEW HOTSPOT IS INTRODUCED TO THE CONFIG FILE
#
def insert_hotspot_details(table, record):
    response = table.put_item(
       Item = {
            'address': record['address'],
            'name': record['name'],
            'born_date': record['born_date'],
            'latest_processed_date': record['latest_processed_date'],
            'earliest_processed_date': record['earliest_processed_date'],
            'activity_count': record['activity_count']
        }
    )

    if 'ResponseMetadata' in response:
        if 'HTTPStatusCode' in response['ResponseMetadata']:
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.info("SUCCESS - insert_hotspot_details - Inserting: " + str(record))
                return True

    logger.error("FAILED - insert_hotspot_details - Inserting: " + str(response))
    return False

#
# USED BY THE HISTORIC ACTIVITY PROCESSING
# WHEN ALL ACTIVITY HAS BEEN FETCHED BACK TO THE HOTSPOT BIRTH DATE
# THEN THE HISTORIC PROCESSING WILL NO LONGER BE PERFORMED FOR THAT 
#
def update_hotspot_earliest_processed_date(table, address, date):
    response = table.update_item(
        Key={
            'address': address
        },
        UpdateExpression="set earliest_processed_date=:lpd",
        ExpressionAttributeValues={
            ':lpd': date
        },
        ReturnValues="UPDATED_NEW"
    )

    if 'ResponseMetadata' in response:
        if 'HTTPStatusCode' in response['ResponseMetadata']:
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.debug("SUCCESS - update_hotspot_earliest_processed_date - Updating: " + str(date))
                return True

    logger.error("FAILED - update_hotspot_earliest_processed_date - Updating: " + str(response))
    return False

