#!/bin/bash

# Remove all previous MongoDB repository configurations
sudo rm -f /etc/apt/sources.list.d/*mongo*.list

# Clean APT cache to remove any cached repository data
sudo apt-get clean

# Update package list
sudo apt-get update || { echo "Failed to update package list"; exit 1; }

# Install dependencies
sudo apt-get install -y gnupg curl

# Import MongoDB public GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

# Add MongoDB repository
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | \
   sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

# Update package list again
sudo apt-get update || { echo "Failed to update package list"; exit 1; }

# Install MongoDB
sudo apt-get install -y mongodb-org

# Start MongoDB service
sudo systemctl start mongod

# Enable MongoDB to start on boot
sudo systemctl enable mongod

# Check if MongoDB is installed and running
if command -v mongod >/dev/null 2>&1 && sudo systemctl is-active --quiet mongod; then
    echo "MongoDB installed and running successfully!"
    # Optionally display MongoDB version
    mongod --version | head -n 1
else
    echo "MongoDB installation failed or service is not running."
    echo "Please check the logs with: sudo journalctl -u mongod"
    exit 1
fi