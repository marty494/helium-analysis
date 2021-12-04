import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = 'us-east-2'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
headers = { "Content-Type": "application/json" }
host = 'https://search-helium-domain-7guwiwhhsj7tz6ztjrlmbdmyyi.us-east-2.es.amazonaws.com/'


def document_exists(index, document_id):
    url = host + index + '/_doc/' + document_id
    r = requests.head(url, auth=awsauth)
    
    print(r)
    print('document_exists -  index: [' + index + '], document_id: [' + document_id + ']')

    return True


#
# WRITE THE DOCUMENT TO OPENSEARCH
# WILL RETURN A CONFLICT STATUS CODE IF THE ENTRY ALREADY EXISTS
#
def write_document(index, document, document_id):
    url = host + index + '/_create/' + document_id
    r = requests.put(url, auth=awsauth, json=document, headers=headers)

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
# UTILITY METHOD - MANUAL USE ONLY
#
def delete_all(index):
    url = host + index + '/_delete_by_query?conflicts=proceed'
    document = {
        'query': {
            'match_all': {}
        }
    }
    response = requests.post(url, auth=awsauth, json=document, headers=headers)
    logger.info(response.text)

#
# UNUSED - FOR POSSIBLE FUTURE USE
#
def read_document(index):
    url = host + index + '/_search'
    # Put the user query into the query DSL for more accurate search results.
    # Note that certain fields are boosted (^).
    query = {
        "size": 25,
        "query": {
            "multi_match": {
                "query": event['queryStringParameters']['q'],
                "fields": ["type^4", "time^2", "rewards"]
            }
        }
    }

    # Elasticsearch 6.x requires an explicit Content-Type header
    #headers = { "Content-Type": "application/json" }

    # Make the signed HTTP request
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }

    # Add the search results to the response
    response['body'] = r.text
    return response
