import logging
from pymongo import MongoClient

# ======== Logger Setup =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/extract_ips.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ======== MongoDB Setup =========
client = MongoClient("mongodb://localhost:27017")
db = client["countly"]
summary_col = db["summary"]
ip_col = db["distinct_ips"]

# ======== Clear old data (optional) ========
ip_col.drop()
logger.info("Dropped old 'distinct_ips' collection if it existed.")

# ======== Aggregation Pipeline =========
pipeline = [
    {
        "$match": {
            "ip": { "$exists": True, "$ne": None, "$ne": "" }
        }
    },
    {
        "$group": {
            "_id": "$ip"
        }
    },
    {
        "$project": {
            "_id": 0,
            "ip": "$_id",
            "location": None,   # For future enrichment
            "status": "pending" # Optional status field to mark enrichment state
        }
    }
]

logger.info("Starting aggregation to extract distinct IPs...")
cursor = summary_col.aggregate(pipeline, allowDiskUse=True)

batch = []
batch_size = 1000
count = 0

for doc in cursor:
    batch.append(doc)
    if len(batch) == batch_size:
        ip_col.insert_many(batch)
        count += len(batch)
        logger.info(f"Inserted {count} IPs so far...")
        batch.clear()

# Insert remaining
if batch:
    ip_col.insert_many(batch)
    count += len(batch)
    logger.info(f"Inserted final batch. Total inserted: {count}")

logger.info("✅ Done extracting distinct IPs.")
