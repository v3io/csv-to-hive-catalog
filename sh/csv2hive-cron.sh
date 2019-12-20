#!/bin/bash
source conf/env.conf

export KUBECTL_PATH=/usr/local/bin/
export PATH=$PATH:${KUBECTL_PATH}

utc_time=$(date +%Y-%m-%d-%H-%M)
log_dir="$(pwd)""/logs/"
mkdir -p ${log_dir}
log_file=${log_dir}"csv2hive.log."${utc_time}

#ngnix host name
nginx_host=`kubectl -n default-tenant get ingress --no-headers -o custom-columns=":spec.rules[0].host" | grep webapi`

shell_container=`kubectl -n default-tenant get pods --no-headers -o custom-columns=":metadata.name" | grep shell`
echo $shell_container 2>&1 | tee -a $log_file

#spark-submit
command="chmod 755 /v3io/bigdata/csv2hive/sh/generate_hive_tables.sh;/v3io/bigdata/csv2hive/sh/./generate_hive_tables.sh $nginx_host ${username} ${password}"
kubectl -n default-tenant exec -it $shell_container -- /bin/bash -c "$command" 2>&1 | tee -a $log_file