#!/bin/bash

#   1. csvs_folder_path
#   2. table_name
#   3. project_name
#   4. bq_dataset
#   5. gcs_bucket_name


if [ $# -ne 5 ]; then
  echo "Usage : $0 csvs_folder_path table_name gcp_project bq_dataset gcs_bucket_name"
  exit 1
fi

csvs_folder_path=$1
table_name=$2
project_name=$3
bq_dataset=$4
gcs_bucket_name=$5

python get_schemas.py "$csvs_folder_path" "$table_name" 
