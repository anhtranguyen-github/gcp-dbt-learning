#!/bin/bash

set -e  

FILE_NAME="glamira_ubl_oct2019_nov2019.tar.gz"
GCS_PATH="gs://raw-glamira-dec/$FILE_NAME"
LOCAL_DUMP_DIR="dump/countly"
MONGO_DB_NAME="countly"

# Check if file already exists
if [ -f "$FILE_NAME" ]; then
    echo " File '$FILE_NAME' already exists. Skipping download."
else
    echo " Downloading $FILE_NAME from GCS..."
    if gsutil cp "$GCS_PATH" .; then
        echo " Download completed."
    else
        echo " Failed to download $FILE_NAME from GCS."
        exit 1
    fi
fi

# Extract tar.gz
echo " Extracting $FILE_NAME..."
if tar -xzvf "$FILE_NAME"; then
    echo " Extraction completed."
else
    echo " Failed to extract $FILE_NAME."
    exit 1
fi

# Import into MongoDB
echo "🛢️ Importing data into MongoDB..."
if [ -d "$LOCAL_DUMP_DIR" ]; then
    if mongorestore --db "$MONGO_DB_NAME" "$LOCAL_DUMP_DIR"; then
        echo " MongoDB restore completed successfully."
    else
        echo " Failed to restore MongoDB database."
        exit 1
    fi
else
    echo " Dump directory '$LOCAL_DUMP_DIR' not found. Restore aborted."
    exit 1
fi
