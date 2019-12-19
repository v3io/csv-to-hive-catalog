import requests
import os
import json
import collections
import time
from requests.auth import HTTPBasicAuth

CONTAINER_NAME = os.environ.get('CONTAINER_NAME', "bigdata")
IGZ_PASS = os.environ.get('IGZ_PASS', "datal@ke!")
IGZ_USER = os.environ.get('IGZ_USER', "iguazio")
CSV_TO_HIVE_KV = os.environ.get('CSV_TO_HIVE_KV', "csv_to_hive")
NGINX_HOST = os.environ.get('NGINX_HOST', 'v3io-webapi.default-tenant.svc')
NGINX_PORT = os.environ.get('NGINX_PORT', '8081')


def get_kv_url(kv_name):
    return 'http://{0}:{1}/{2}/{3}/'.format(NGINX_HOST, NGINX_PORT, CONTAINER_NAME, kv_name)


def get_function_headers(v3io_function):
    return {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-cache',
    'X-v3io-function': v3io_function
    }


def get_structured_message(csv_location):
    epoch_time = time.time()
    key_field = csv_location.replace('/', '_')+"_"+str(epoch_time)
    json_dict = collections.OrderedDict([
        ('Key', collections.OrderedDict([('id', {'S': key_field})])),
        ('Item', collections.OrderedDict([
          ('location', {'S': csv_location}),
          ('status', {'S': "new"}),
          ('table_name', {'S': ""})
        ]))
      ])
    return json_dict


def ingest_to_kv(csv_location, context):
    context.logger.info('Inside ingestion function ....')
    payload = json.dumps(get_structured_message(csv_location), indent=4)
    url = get_kv_url(CSV_TO_HIVE_KV)
    headers = get_function_headers('PutItem')
    response = requests.post(url, auth=HTTPBasicAuth(IGZ_USER, IGZ_PASS), data=payload, headers=headers, timeout=5)
    if response.status_code != 200:
        context.logger.error(
            "ingestion failed with status code %s for record %s", response.status_code, response.request.body
        )


def handler(context, event):
    try:
        context.logger.info('Inside csv to hive function handler ....')
        csv_path = event.body.decode('utf-8')
        if csv_path.strip():
            ingest_to_kv(csv_path, context)
            return context.Response(body='csv-to-hive entry registered successful', headers={},
                                    content_type='text/plain', status_code=200)
        return context.Response(body='csv-to-hive entry failed as path is missing', headers={},
                                content_type='text/plain', status_code=300)
    except Exception as e:
        context.logger.error('csv path ingestion job failed', e)
        return context.Response(body='error occur while writing to kv', headers={}, content_type='text/plain', status_code=500)
