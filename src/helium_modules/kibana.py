import os
import json
import requests
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

headers = { "Content-Type": "application/json" }
host = 'http://es01-dev:9200/'

#
# LOOKUP A DOCUMENT AND RETURN TRUE IF IT IS FOUND
#
def document_exists(index, document_id):
    url = host + index + '/_doc/' + document_id
    response = requests.head(url)
   
    logger.debug('document_exists() response: ' + str(response))
    logger.debug('document_exists() index: ' + index + ', document_id: ' + document_id)

    return True


#
# WRITE DOCUMENT TO ELASTICSEARCH
# RETURN A CONFLICT STATUS CODE IF THE ENTRY ALREADY EXISTS
#
def write_document(index, document, document_id):
    url = host + index + '/_create/' + document_id
    r = requests.put(url, json=document, headers=headers)

    if r.status_code == requests.codes.created:
        logger.debug('CREATED - write_document - Created document: ' + str(document))
        return True

    if r.status_code == requests.codes.conflict:
        response = r.json()
        if 'error' in response:
            if 'type' in response['error']:
                if response['error']['type'] == 'version_conflict_engine_exception':
                    # Already exists, just skip
                    logger.debug("SKIPPED - write_document - Already exists: " + str(document))
                    return True

    logger.error('FAILED - write_document - status_code: ' + str(r.status_code) + ', Error creating: ' + str(response))

    return False

#
# RETRIEVE DOCUMENT FROM ELASTICSEARCH
#
def get_document(index, hotspot_address):
    uri = host + index + '/_doc/' + hotspot_address

    logger.info('uri: ' + uri)

    response = requests.get(uri, headers=headers)

    try:
        document = json.loads(response.text)

    except:
        document = response.text

    return document

