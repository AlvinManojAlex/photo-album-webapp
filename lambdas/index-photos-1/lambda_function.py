import json
import urllib.parse
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import os
from datetime import datetime


print('Loading function')

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

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')

def add_to_opensearch(key, bucket, labels):
    doc = {
        "objectKey": key,
        "bucket": bucket,
        "createdTimestamp": datetime.now(),
        "labels": labels
    }

    response = opensearch.index(
        index=opensearch_index,
        body=doc
    )

    return {
        "status": "ok",
        "opensearch_response": response
    }

def detect_labels(photo, bucket, custom_labels):
    print(f"Detecting labels for bucket: {bucket} and photo: {photo}")
    response = rekognition.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}}, MaxLabels=10)
    labels = custom_labels

    print('Detected labels for ' + photo)
    
    for label in response['Labels']:
        labels.append(label['Name'].lower())

    return labels

def lambda_handler(event, context):
    try:
        print("Received event: " + json.dumps(event, indent=2))

        bucket = event['Records'][0]['s3']['bucket']['name']
        photo = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        # custom_labels = event['headers'].get('x-amz-meta-customLabels', '')
        # custom_labels_list = [l.strip() for l in custom_labels.split(',')] if custom_labels else []

        obj = s3.head_object(Bucket=bucket, Key=photo)
        custom_labels = obj['Metadata'].get('customlabels', '')
        custom_labels_list = [l.strip() for l in custom_labels.split(',')] if custom_labels else []
        
        labels = detect_labels(photo, bucket, custom_labels_list)
        print(labels)

        res = add_to_opensearch(photo, bucket, labels)
        
        return {
            'statusCode':200,
            'headers': {"Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"*","Access-Control-Allow-Headers": "*"},
            'body': json.dumps({
                "message": "Success",
            })
        }
    
    except Exception as e:
        print('Error for object {} from bucket {}'.format(photo, bucket))
        print(e)
        
        return {
            'statusCode':500,
            'headers': {"Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"*","Access-Control-Allow-Headers": "*"},
            'body': json.dumps({
                "message": "Failure",
                "error": str(e)
            })
        }

