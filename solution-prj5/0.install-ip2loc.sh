#!/bin/bash

# Đóng script khi gặp lỗi
set -e


URL="https://download.ip2location.com/lite/IP2LOCATION-LITE-DB1.BIN.ZIP"
FILENAME="IP2LOCATION-LITE-DB1.BIN.ZIP"
TARGET_DIR="ip2loc"

# Function to handle errors
error_exit() {
    echo "Error: $1" >&2
    exit 1
}

if [ ! -d "$TARGET_DIR" ]; then
    echo "Creating target directory: $TARGET_DIR"
    mkdir -p "$TARGET_DIR" || error_exit "Failed to create directory $TARGET_DIR"
fi

# 
cd "$TARGET_DIR" || error_exit "Failed to change to directory $TARGET_DIR"


echo "Downloading $FILENAME to $TARGET_DIR..."
wget "$URL" -O "$FILENAME" || error_exit "Failed to download $FILENAME"

# Download unzip
if ! command -v unzip &> /dev/null; then
    echo "Installing unzip..."
    sudo apt update || error_exit "Failed to update package list"
    sudo apt install -y unzip || error_exit "Failed to install unzip"
fi

# Unzip 
echo "Unzipping $FILENAME in $TARGET_DIR..."
unzip "$FILENAME" || error_exit "Failed to unzip $FILENAME"

# Cleanup zipped file
echo "Cleaning up..."
rm -f "$FILENAME" || error_exit "Failed to remove $FILENAME"

echo "Script completed successfully."