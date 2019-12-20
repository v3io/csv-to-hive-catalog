import os
from pyspark.sql import SparkSession
import logging
import json
import requests
import urllib3
import collections
import time
import sys

from requests.auth import HTTPBasicAuth

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NGINX_HOST = sys.argv[1]
IGZ_USER = sys.argv[2]
IGZ_PASS = sys.argv[3]

CONTAINER_NAME = os.environ.get('CONTAINER_NAME', "bigdata")

CSV_TO_HIVE_KV = os.environ.get('CSV_TO_HIVE_KV', "csv_to_hive_catalog")
HIVE_SCHEMA = os.environ.get('HIVE_SCHEMA', 'default')

logger = logging.getLogger('generate-hive-tables')
spark = SparkSession.builder.appName("Import parquet schema to hive").config("hive.metastore.uris", "thrift://hive:9083").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = []


def get_kv_url(kv_name):
    return 'https://{0}/{1}/{2}/'.format(NGINX_HOST, CONTAINER_NAME, kv_name)


def get_function_headers(v3io_function):
    return {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        'X-v3io-function': v3io_function
    }


def get_structured_message(marker):

    json_dict = collections.OrderedDict(
        ({'AttributesToGet': "location,id",
          'FilterExpression': "(status == 'new')",
          'Marker': marker})
        )
    return json_dict


def get_structured_message_update(table_name):

    update_expression = "SET status='created';SET table_name='"+table_name+"';"
    json_dict = collections.OrderedDict(
        ({'UpdateMode': "CreateOrReplaceAttributes",
          'ConditionExpression': "(status == 'new')",
          'UpdateExpression': update_expression})
        )
    return json_dict


def get_csv_paths(marker = ""):
    global data
    try:
        json_dict = json.dumps(get_structured_message(marker), indent=4)
        response = requests.post(get_kv_url(CSV_TO_HIVE_KV),
                                 auth=HTTPBasicAuth(IGZ_USER, IGZ_PASS), data=json_dict, headers=get_function_headers('GetItems'), verify=False)
        if response.status_code == 200:
            parsed_response = json.loads(response.text)
            last_item_included = parsed_response['LastItemIncluded']
            items = parsed_response['Items']
            data = data + items
            if last_item_included == "FALSE":
                next_marker = parsed_response['NextMarker']
                get_csv_paths(next_marker)
        return data
    except Exception as e:
        logger.error(str(e))
        pass


def update_status(key, table_name):
    try:
        json_dict = json.dumps(get_structured_message_update(table_name), indent=4)
        response = requests.post(get_kv_url(CSV_TO_HIVE_KV)+key,
                                 auth=HTTPBasicAuth(IGZ_USER, IGZ_PASS), data=json_dict, headers=get_function_headers('UpdateItem'), verify=False)
        if response.status_code == 200:
            logger.info("table created successfully")
    except Exception as e:
        logger.error("update failed for hive table", str(e))
        pass


def getCreateTableScriptCSV(databaseName, tableName, path, df):
    cols = df.dtypes
    create_script = "CREATE EXTERNAL TABLE " + databaseName + "." + tableName + "("
    col_array = []
    for colName, colType in cols:
        col_array.append(colName.replace(" ", "_") + " " + colType)
    create_cols_script = ", ".join(col_array)
    
    script = create_script + create_cols_script + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '" + path + "' TBLPROPERTIES('skip.header.line.count'='1') "
    logger.info(script)
    return script

   
def createTableCSV(databaseName, tableName, path): 
    df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(path)
    sqlScript = getCreateTableScriptCSV(databaseName, tableName, path, df)
    spark.sql(sqlScript)


def main():
    items = get_csv_paths()
    if not items:
        print("no tables to be generated")
    # Set path where the csv file located
    for item in items:
        path = item['location']['S']
        key = item['id']['S']
        my_csv_file_path = os.path.join("v3io://" + path)
        logger.info(my_csv_file_path)
        file_name = os.path.splitext(os.path.basename(my_csv_file_path))[0]
        millis = str(round(time.time() * 1000))
        table_name = file_name + "_" + millis
        try:
            createTableCSV(HIVE_SCHEMA, table_name, my_csv_file_path)
            update_status(key, table_name)
            print("table created successfully for path: ", my_csv_file_path)
        except Exception as e:
            logger.error("table creation failed for path :"+path)
            logger.error("error is "+str(e))
            pass


if __name__ == '__main__':
    main()
