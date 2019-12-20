#!/bin/bash
source conf/env.conf


#copy source files to v3io
command1="curl --request PUT  --url ${v3io_api_endpoint_host}/${v3io_container}/csv2hive/src/generate_hive_tables.py -H x-v3io-session-key:${access_key} -H Content-Type:application/x-www-form-urlencoded --insecure --data $PWD/src/generate_hive_tables.py"

#copy dependency files to v3io
command2="curl --request PUT  --url ${v3io_api_endpoint_host}/${v3io_container}/csv2hive/config/requirements.txt -H x-v3io-session-key:${access_key} -H Content-Type:application/x-www-form-urlencoded --insecure --data $PWD/conf/requirements.txt"

#copy sh files to v3io
command3="curl --request PUT  --url ${v3io_api_endpoint_host}/${v3io_container}/csv2hive/sh/generate_hive_tables.sh -H x-v3io-session-key:${access_key} -H Content-Type:application/x-www-form-urlencoded --insecure --data $PWD/sh/generate_hive_tables.sh"

$command1
$command2
$command3

#crontab
chmod 755 "$PWD"/sh/csv2hive-cron.sh
cron_command="*/2 * * * * $PWD/sh/csv2hive-cron.sh"
crontab -l > mycron
#echo new cron into cron file
echo "${cron_command}" >> mycron
#install new cron file
crontab mycron
rm mycron

