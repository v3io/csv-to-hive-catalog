#!/bin/bash

if ! [ -d /User/csv2hiveenv  ] ; then
        mkdir csv2hiveenv
        pip install virtualenv --user -U --no-cache-dir
        /User/.local/bin/virtualenv csv2hiveenv
        source /User/csv2hiveenv/bin/activate
        pip install -r /v3io/bigdata/csv2hive/config/requirements.txt
else
        source /User/csv2hiveenv/bin/activate
fi

#ngnix host name 
nginx_host=$1

python /v3io/bigdata/csv2hive/src/generate_hive_tables.py ${nginx_host}