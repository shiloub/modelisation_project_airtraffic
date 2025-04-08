#!/bin/bash

if [ $# -ne 4 ]; then
  echo "Usage : $0 number gcp_project bq_dataset table_name"
  exit 1
fi

bq mk --external_table_definition=./jsons/$4_schema_EXTERNAL$1_def.json $2:$3.$4_EXTERNAL$1