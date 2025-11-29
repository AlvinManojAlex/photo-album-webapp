import os
import json, boto3
from datetime import datetime
import urllib.parse
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = os.environ['OPENSEARCH_URL']
opensearch_index = 'photos'
opensearch = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

lex = boto3.client('lexv2-runtime', region_name=region)
BOT_ID = os.environ['BOT_ID']
BOT_ALIAS_ID = os.environ['BOT_ALIAS_ID']
LOCALE_ID = 'en_US'

s3 = boto3.client('s3')
S3_URL = os.environ['S3_URL']

def get_slots_from_lex(query):
    slots = []
    
    response = lex.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId="user_lambda_search_photos",
            text=query
        )
    
    print("lex response: ", response)
    
    if not response['sessionState']['intent']['slots'] == None:
        for key,value in response['sessionState']['intent']['slots'].items():
            if value != None:
                slots.append(value['value']['interpretedValue'].lower())

    return slots

def get_presigned_url(bucket, key):
    url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket,
            'Key': key
        },
        ExpiresIn=3600
    )
    return url

def search_elastic_search(slots):
    keys = []
    results = []

    query = {
        "size": 10,
        "query": {
            "terms": {
                "labels.keyword": slots
            }
        }
    }

    response = opensearch.search(index=opensearch_index, body=query)

    print("opensearch response: ", response)

    hits = response.get('hits', {}).get('hits', [])
    for hit in hits:
        source = hit.get('_source', {})
        results.append({
            # "url1": f"{S3_URL}/{source.get('bucket')}/{source.get('objectKey')}",
            "url": get_presigned_url(source.get('bucket'), source.get('objectKey')),
            "labels": source.get('labels')
        })

    return results

def lambda_handler(event, context):
    try:
        q = event['queryStringParameters']['q']
        print(f"query: {q}")

        slots = get_slots_from_lex(q)
        print("labels", slots)

        img_paths = []
        if len(slots):
            img_paths = search_elastic_search(slots)

        print(img_paths)
        
        return{
            'statusCode':200,
            'headers': {"Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"*","Access-Control-Allow-Headers": "*"},
            'body': json.dumps({
                "message": "Success",
                "data": img_paths
            })
        }

    except Exception as e:
        print(e)
        return{
            'statusCode':500,
            'headers': {"Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"*","Access-Control-Allow-Headers": "*"},
            'body': json.dumps({
                "message": "Failure",
                "error": str(e)
            })
        }
