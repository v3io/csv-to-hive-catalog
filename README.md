# csv-to-hive-catalog
this utility allows you to automatically create external hive tables on the csv files ingested in v3io.

## Setup :
#### Deployment of Cron
currently the cron is set to run every 2 minutes
1) git clone https://github.com/v3io/csv-to-hive-catalog.git
2) cd csv-to-hive-catalog
3) chmod +x csv2hive-deployment.sh
4) edit the conf/env.conf file as per environment
5) ./csv2hive-deployment.sh

#### Deployment of nuclio function
nuclio function can be deployed by using the import functionality of nuclio UI.
the function yaml is located in nuclio/csv2hive.yaml

#### Usage:
use the nuclio function endpoints to send the path of the csv as the payload in the following format.
eg1. curl <nulcio_function_endpoint> -d '<container_name>/folder1/sample.csv'
eg2. curl <nulcio_function_endpoint> -d '<container_name>/folder1/

curl http://3.1.134.4:30433/ -d 'bigdata/new.csv'

##### Hive catalog will be accessible from the kv table --> csv_to_hive_catalog

![sampel KV table](../master/images/hive_catalog_kv.png)