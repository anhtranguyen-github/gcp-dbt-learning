import logging
import os
import sys
from pymongo import MongoClient

# Logger
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
    results = []

    total_docs = collection.count_documents({})
    msg = f"Collection '{collection_name}' has {total_docs} documents."
    logger.info(msg)
    results.append(msg)

    # Gather keys from a sample of documents for performance
    keys = set()
    sample_cursor = collection.find({}, limit=100)
    for doc in sample_cursor:
        keys.update(doc.keys())

    # Profile each key: null/empty counts and distinct values count using aggregation
    for key in keys:
        count_null = collection.count_documents({key: {"$in": [None, ""]}})
        pipeline = [
            {"$match": {key: {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": f"${key}"}},
        ]
        agg_result = list(collection.aggregate(pipeline, allowDiskUse=True))
        distinct_count = len(agg_result)
        msg = f"Field '{key}' in '{collection_name}': {distinct_count} distinct values, {count_null} null/empty values."
        logger.info(msg)
        results.append(msg)

    # Additional profiling: view distribution for "status" field if present using aggregation
    if "status" in keys:
        pipeline = [
            {"$match": {"status": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        ]
        status_results = list(collection.aggregate(pipeline, allowDiskUse=True))
        for res in status_results:
            status = res["_id"]
            count_status = res["count"]
            msg = f"Status '{status}' in '{collection_name}': {count_status} documents."
            logger.info(msg)
            results.append(msg)

    return results


if __name__ == "__main__":
    try:
        client = MongoClient()
        db = client["countly"]

        collections_to_profile = ["product_names", "distinct_ips"]
        all_results = []
        for coll in collections_to_profile:
            logger.info(f"Profiling collection: {coll}")
            all_results.extend(profile_collection(db, coll))

        # Write collected profiling results to an output file
        output_file = "data_profiling_output.txt"
        with open(output_file, "w") as f:
            for line in all_results:
                f.write(line + "\n")
        logger.info(f"Profiling output written to {output_file}")

    except Exception as e:
        logger.error(f"Error during profiling: {e}")
        sys.exit(1)
