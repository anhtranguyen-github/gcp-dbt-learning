import csv
from pymongo import MongoClient


client = MongoClient(
    "mongodb://localhost:27017/"
)  # Replace with your actual MongoDB URI
db = client["countly"]
collection = db["product_names"]

documents = collection.find({}, {"product_id": 1, "product_name": 1, "_id": 0})

csv_file = "product_data.csv"

with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=["product_id", "product_name"])
    writer.writeheader()
    for doc in documents:
        writer.writerow(doc)

print(f"Data has been saved to {csv_file}")
