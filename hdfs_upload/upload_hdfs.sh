#!/bin/bash

# Define the base directory and date
BASE_DIRECTORY="../results/"
TODAY_DATE=$(date "+%Y-%m-%d")
MONTH=$(date "+%Y-%m")

# Define the directory and file path
MONTHLY_DIRECTORY="${BASE_DIRECTORY}${MONTH}/"
FILE_NAME="stocks_data_${TODAY_DATE}.csv"
FILE_PATH="${MONTHLY_DIRECTORY}${FILE_NAME}"

# Define the HDFS destination path
HDFS_PATH="/path/to/hdfs/${MONTH}/"

# Check if the file exists
if [ -f "$FILE_PATH" ]; then
    # Ensure the directory exists on HDFS
    hdfs dfs -mkdir -p $HDFS_PATH

    # Upload the file to HDFS
    hdfs dfs -put -f $FILE_PATH $HDFS_PATH
    echo "File ${FILE_NAME} uploaded successfully to HDFS."
else
    echo "File ${FILE_NAME} does not exist."
fi
