import logging
import os
import sys
from pymongo import MongoClient

# Logger setup (reusing log configuration from 6.documentaion-testing.py)
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/data_profiling.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

def profile_collection(db, collection_name):
    collection = db[collection_name]
    
    # Total document count
    total_docs = collection.count_documents({})
    logger.info(f"Collection '{collection_name}' has {total_docs} documents.")
    
    # Gather keys from a sample of documents for performance
    keys = set()
    sample_cursor = collection.find({}, limit=100)
    for doc in sample_cursor:
        keys.update(doc.keys())
    
    # Profile each key: null/empty counts and distinct values count
    for key in keys:
        count_null = collection.count_documents({key: {"$in": [None, ""]}})
        distinct_vals = collection.distinct(key)
        logger.info(
            f"Field '{key}' in '{collection_name}': {len(distinct_vals)} distinct values, {count_null} null/empty values."
        )
    
    # Additional profiling: view distribution for "status" field if present
    if "status" in keys:
        status_values = collection.distinct("status")
        for status in status_values:
            count_status = collection.count_documents({"status": status})
            logger.info(
                f"Status '{status}' in '{collection_name}': {count_status} documents."
            )

if __name__ == "__main__":
    try:
        client = MongoClient()  # Adjust connection parameters if necessary
        db = client["countly"]

        # List the collections to profile. You can add 'summary' or others if needed.
        collections_to_profile = ["product_names", "distinct_ips"]
        for coll in collections_to_profile:
            logger.info(f"Profiling collection: {coll}")
            profile_collection(db, coll)
    except Exception as e:
        logger.error(f"Error during profiling: {e}")
        sys.exit(1)