#!/bin/bash

# Remove old mongo versions
sudo rm -f /etc/apt/sources.list.d/*mongo*.list

sudo apt-get clean
sudo apt-get update || { echo "Failed to update package list"; exit 1; }

# Install mongodb
sudo apt-get install -y gnupg curl

curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | \
   sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list


sudo apt-get update || { echo "Failed to update package list"; exit 1; }


sudo apt-get install -y mongodb-org

# Start MongoDB service
sudo systemctl start mongod

sudo systemctl enable mongod

# Check if MongoDB is installed and running
if command -v mongod >/dev/null 2>&1 && sudo systemctl is-active --quiet mongod; then
    echo "MongoDB installed and running successfully!"
    mongod --version | head -n 1
else
    echo "MongoDB installation failed or service is not running."
    echo "Please check the logs with: sudo journalctl -u mongod"
    exit 1
fi