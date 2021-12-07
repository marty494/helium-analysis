import os
import json
import decimal
from datetime import datetime
from datetime import date
import helium_modules.helium_api as api
import helium_modules.kibana as kibana
import helium_modules.config as config
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

#
# THE MAIN PROCESSING LOOP - PROCESSES EACH CONFIGURED HOTSPOT
#
def process_hotspots(run_date):
    hotspots = config.get_hotspots()

    for hotspot in hotspots:
        process_hotspot(hotspot['hotspot_address'], run_date)

#
# PROCESSES THE SUPPLIED HOTSPOT
# RETRIEVES AND POPULATES HOTSPOT DETAILS IF NECESSARY
# PERFORMS ACTIVITY EXTRACTION:
#
def process_hotspot(hotspot_address, run_date):
    logger.debug('Processing hotspot: ' + hotspot_address + ', for run_date: ' + run_date.astimezone().isoformat())

    try:
        hotspot_details = config.get_hotspot_details(hotspot_address)

    except Exception as error:
        logger.error('process_hotspot() error: ' + str(error))
        return

    logger.debug('process_hotspot() name: [' + hotspot_details['name'] + 
        '], born_date: [' + hotspot_details['born_date'] +
        '], processed_date: [' + hotspot_details['processed_date'] +
        '], activity_count: [' + hotspot_details['activity_count'] + ']')

    exit()

        
    # temp
    if hotspot_details['name'] == 'prehistoric-bone-yeti':
        if hotspot_details['earliest_processed_date'] == hotspot_details['born_date']:
            logger.debug('earliest_processed_date = ' + hotspot_details['earliest_processed_date'])
            logger.debug('born_date = ' + hotspot_details['born_date'])

            result = process_latest_activity(hotspot_address, hotspot_details, run_date)

            if result == True:
                logger.debug("SUCCESS - Processed periodic data")
            else:
                logger.error("FAILURE - Error processing periodic data")
        else:
            result = process_historic_activity(hotspot_address, hotspot_details)

            if result == True:
                logger.debug("SUCCESS - Fully updated historic data")
            else:
                logger.error("FAILURE - Error updating historic data")

#
# PERIODIC SCHEDULED ACTIVITY RETRIEVAL
# READS FROM THE RUN DATE BACK TO THE LATEST PROCESSED DATE
# THE LATEST DATE IS UPDATED AFTER SUCCESSFUL STORAGE OF THE ACTIVITY
#
def process_latest_activity(hotspot_address, hotspot_details, run_date):
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
def process_historic_activity(hotspot_address, hotspot_details):
    logger.debug('Processing historic activity - hotspot: ' + hotspot_details['name'])

    min_date = hotspot_details['born_date'] # Read back to birth for historic data
    max_date = hotspot_details['earliest_processed_date'] # Read from the current earliest date backwards
    response = api.get_hotspot_activity(hotspot_address, min_date, max_date)

    if 'data' in response:
        if len(response['data']) > 0:
            result = persist_data(hotspot_address, hotspot_details['name'], response['data'])
            if result == False:
                logger.error('FAILED - process_historic_activity - Persisting data: ' + str(response))
                return False

    while 'cursor' in response:
        response = api.get_hotspot_activity_cursor(hotspot_address, response['cursor'])
        if 'data' in response:
            if len(response['data']) > 0:
                result = persist_data(hotspot_address, hotspot_details['name'], response['data'])
                if result == False:
                    logger.error('FAILED - process_historic_activity - Persisting cursor: ' + str(response))
                    return False

    result = config.update_hotspot_earliest_processed_date(hotspot_address, hotspot_details['born_date'])

    return result

#
# PREPARE AND PERSIST THE ACTIVITY
#
def persist_data(hotspot_address, index, data):
    logger.debug('persist_data - index: ' + index)
    for document in data:
        logger.debug('persist_data - document: ' + str(document))
        if 'hash' in document and 'time' in document:
            exists = kibana.document_exists(index, document['hash'])
            if exists:
                return True
            document = transform_time(document)
            result = kibana.write_document(index, document, document['hash'])
            if result == True:
                result = config.update_hotspot_earliest_processed_date(hotspot_address, document['time'])
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

#
# THE ENTRY POINT WHEN LAUNCHING
#
if __name__ == '__main__':
    run_date = datetime.now()
    logger.info('Loading Helium function at time: ' + run_date.astimezone().isoformat())
    process_hotspots(run_date)

