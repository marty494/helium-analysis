import os
import json
import decimal
from datetime import datetime
from datetime import date
import helium_modules.hotspot_dao as dao
import helium_modules.helium_api as api
import helium_modules.opensearch as elastic
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#
# THE ENTRY POINT FOR AWS LAMBDA
# CALLED BY AN EVENT - A TEST EVENT OR SCHEDULED EVENT
#
def lambda_handler(event, context):
    logger.info('Loading Helium function with Event: ' + str(event))

    if not('time' in event):
        return {
            'statusCode': 400,
            'body': json.dumps('Bad Request: No time in event')
        }

    try:
        run_date = event['time']
    except (TypeError, ValueError) as err:
        return {
            'statusCode': 400,
            'body': json.dumps('Bad Request - invalid date or time')
        }

    #clean_up('prehistoric-bone-yeti')
    process_hotspots(run_date)

#
# LOAD IN THE CONFIG FILE WHICH CONTAINS THE HOTSPOT ADDRESSES TO BE PROCESSED
#
def load_config():
    configPath = os.environ['LAMBDA_TASK_ROOT'] + "/config.json"
    configContents = open(configPath).read()
    configJson = json.loads(configContents)
    return configJson

#
# A UTILITY METHOD - FOR MANUAL INVOCATION ONLY
# IT IS USUALLY EASIER TO DELETE DATA FROM THE OPENSEARCH DEVTOOLS CONSOLE
# E.G. DELETE /index-name
#
def clean_up(index):
    elastic.delete_all(index)

#
# THE MAIN PROCESSING LOOP - PROCESSES EACH CONFIGURED HOTSPOT
#
def process_hotspots(run_date):
    configJson = load_config()
    hotspot_table = dao.get_table('hotspot')

    for hotspot in configJson:
        process_hotspot(hotspot_table, hotspot['hotspot_address'], run_date)

#
# PROCESSES THE SUPPLIED HOTSPOT
# RETREIVES AND POPULATES HOTSPOT DETAILS IF NECESSARY
# PERFORMS EITHER:
# - HISTORIC RETRIEVIAL
#   IF THE EARLIEST PROCESSED DATE IS LATER THAN THE BIRTH DATE
# - OTHERWISE, PERIODIC SCHEDULED RETRIEVAL
#
def process_hotspot(hotspot_table, hotspot_address, run_date):
    logger.debug('Processing hotspot: ' + hotspot_address + ', for run_date: ' + run_date)

    hotspot_details = dao.get_hotspot_details(hotspot_table, hotspot_address)

    logger.debug('Name: [' + hotspot_details['name'] + 
        '], born_date: [' + hotspot_details['born_date'] +
        '], latest_processed_date: [' + hotspot_details['latest_processed_date'] +
        '], earliest_processed_date: [' + hotspot_details['earliest_processed_date'] + ']')

    if hotspot_details['name'] == '':
        hotspot_data = api.get_hotspot_data(hotspot_address)
        
        if hotspot_data['name'] == '':
            logger.debug('SKIPPING - Could NOT get hotspot details from Helium API')
            return

        activity_count = api.get_hotspot_activity_count(hotspot_address)

        logger.debug('Adding hotspot details to DynamoDB for address: ' + hotspot_address)
        hotspot_details['name'] = hotspot_data['name']
        hotspot_details['born_date'] = hotspot_data['timestamp_added']
        hotspot_details['latest_processed_date'] = ''
        hotspot_details['earliest_processed_date'] = run_date
        hotspot_details['activity_count'] = activity_count

        hotspot_record = {
            'address': hotspot_address,
            'name': hotspot_details['name'],
            'born_date': hotspot_details['born_date'],
            'latest_processed_date': hotspot_details['latest_processed_date'],
            'earliest_processed_date': hotspot_details['earliest_processed_date'],
            'activity_count': hotspot_details['activity_count']
        }
        result = dao.insert_hotspot_details(hotspot_table, hotspot_record)
        if result == False:
            return False
        
    # temp
    if hotspot_details['name'] == 'prehistoric-bone-yeti':
        if hotspot_details['earliest_processed_date'] == hotspot_details['born_date']:
            logger.debug('earliest_processed_date = ' + hotspot_details['earliest_processed_date'])
            logger.debug('born_date = ' + hotspot_details['born_date'])

            result = process_latest_activity(hotspot_table, hotspot_address, hotspot_details, run_date)

            if result == True:
                logger.debug("SUCCESS - Processed periodic data")
            else:
                logger.error("FAILURE - Error processing periodic data")
        else:
            result = process_historic_activity(hotspot_table, hotspot_address, hotspot_details)

            if result == True:
                logger.debug("SUCCESS - Fully updated historic data")
            else:
                logger.error("FAILURE - Error updating historic data")

#
# PERIODIC SCHEDULED ACTIVITY RETRIEVAL
# READS FROM THE RUN DATE BACK TO THE LATEST PROCESSED DATE
# THE LATEST DATE IS UPDATED AFTER SUCCESSFUL STORAGE OF THE ACTIVITY
#
def process_latest_activity(hotspot_table, hotspot_address, hotspot_details, run_date):
    logger.debug('Processing latest activity - hotspot: ' + hotspot_details['name'])
    logger.debug('NOT YET IMPLEMENTED')
    if is_hotspot_activity(hotspot_address, hotspot_details):
        logger.info('NEW ACTIVITY')
    else:
        logger.info('NO NEW ACTIVITY')
        
    return True

#
# DETERMINES IF ANY ACTIVITY HAS OCCURRED SINCE THE LAST CHECK
# COMPARES THE ACTIVITY PREVIOUSLY RETRIEVED AGAINST THE NEW ACTIVITY COUNTS
#
def is_hotspot_activity(hotspot_address, hotspot_details):
    activity_count = api.get_hotspot_activity_count(hotspot_address)
    logger.info('OLD Activity count: ' + str(hotspot_details['activity_count']))
    logger.info('NEW Activity count: ' + str(activity_count))
    if str(activity_count) != hotspot_details['activity_count']:
        return True
    else:
        return False

#
# HISTORIC ACTIVITY RETRIEVAL
# READS FROM THE EARLIEST DATE BACK TO THE BIRTH DATE
# THE EARLIEST DATE IS UPDATED AFTER SUCCESSFUL STORAGE OF THE ACTIVITY
#
def process_historic_activity(hotspot_table, hotspot_address, hotspot_details):
    logger.debug('Processing historic activity - hotspot: ' + hotspot_details['name'])

    min_date = hotspot_details['born_date'] # Read back to birth for historic data
    max_date = hotspot_details['earliest_processed_date'] # Read from the current earliest date backwards
    response = api.get_hotspot_activity(hotspot_address, min_date, max_date)

    if 'data' in response:
        if len(response['data']) > 0:
            result = persist_data(hotspot_table, hotspot_address, hotspot_details['name'], response['data'])
            if result == False:
                logger.error('FAILED - process_historic_activity - Persisting data: ' + str(response))
                return False

    while 'cursor' in response:
        response = api.get_hotspot_activity_cursor(hotspot_address, response['cursor'])
        if 'data' in response:
            if len(response['data']) > 0:
                result = persist_data(hotspot_table, hotspot_address, hotspot_details['name'], response['data'])
                if result == False:
                    logger.error('FAILED - process_historic_activity - Persisting cursor: ' + str(response))
                    return False

    result = dao.update_hotspot_earliest_processed_date(hotspot_table, hotspot_address, hotspot_details['born_date'])

    return result

#
# PREPARE AND PERSIST THE ACTIVITY
#
def persist_data(hotspot_table, hotspot_address, index, data):
    logger.debug('persist_data - index: ' + index)
    for document in data:
        logger.debug('persist_data - document: ' + str(document))
        if 'hash' in document and 'time' in document:
            exists = elastic.document_exists(index, document['hash'])
            if exists:
                return True
            document = transform_time(document)
            result = elastic.write_document(index, document, document['hash'])
            if result == True:
                result = dao.update_hotspot_earliest_processed_date(hotspot_table, hotspot_address, document['time'])
                if result == False:
                    return False
            else:
                return False
        else:
            logger.error('FAILED - persist_data - Missing hash and/or time')
            return False

    return True

#
# FOR THE PURPOSING OF TEMPORAL INDEXING IT IS NECESSARY TO TURN THE TIME
# FIELD INTO A DATE RATHER THAN MILLISECONDS. WE COULD HAVE ADDED AN ADDITIONAL
# DATE FIELD, BUT THE TIME IN MS IS NOT OF ANY USE
#
def transform_time(document):
    timeMs = document['time']
    strDate = datetime.fromtimestamp(timeMs)
    document['time'] = strDate.astimezone().isoformat()
    return document

