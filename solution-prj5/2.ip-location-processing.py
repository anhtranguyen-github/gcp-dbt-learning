import os
import logging
from pymongo import MongoClient, errors
import IP2Location

# Logger
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ip_process.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load ip2location db
try:
    db_path = "ip2loc/IP2LOCATION-LITE-DB1.BIN"
    ip2loc = IP2Location.IP2Location(db_path)
    logger.info("IP2Location DB loaded successfully.")
except ValueError as e:
    logger.error(f"Failed to load IP2Location DB: {e}")
    raise


# Process all pending IPs in batches
def enrich_ip_locations(batch_size=5000):
    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client["countly"]
        ip_col = db["distinct_ips"]

        # Fetch all pending IPs
        cursor = ip_col.find({"status": "pending"}, {"ip": 1})
        total_ips = ip_col.count_documents(
            {"status": "pending"}
        )  # Use count_documents instead of cursor.count()
        batch = []
        total_processed = 0
        total_failed = 0

        for ip_doc in cursor:
            batch.append(ip_doc)
            if len(batch) >= batch_size:
                processed, failed = process_batch(batch, ip_col)
                total_processed += processed
                total_failed += failed
                logger.info(f"Progress: {total_processed}/{total_ips} IPs processed.")
                batch = []

        # Process any remaining IPs
        if batch:
            processed, failed = process_batch(batch, ip_col)
            total_processed += processed
            total_failed += failed
            logger.info(f"Progress: {total_processed}/{total_ips} IPs processed.")

        logger.info(
            f"Enrichment completed. Total processed: {total_processed}, Failed: {total_failed}"
        )

    except errors.ConnectionFailure as e:
        logger.error(f"MongoDB connection error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def process_batch(batch, ip_col):
    processed = 0
    failed = 0
    for ip_doc in batch:
        ip = ip_doc["ip"]
        try:
            record = ip2loc.get_all(ip)
            update = {
                "location": {
                    "country_code": record.country_short,
                    "country_name": record.country_long,
                },
                "status": "done",
            }
            ip_col.update_one({"_id": ip_doc["_id"]}, {"$set": update})
            processed += 1
        except Exception as e:
            logger.error(f"Failed to enrich IP {ip}: {e}")
            ip_col.update_one({"_id": ip_doc["_id"]}, {"$set": {"status": "error"}})
            failed += 1

    logger.info(
        f"Processed batch of {len(batch)}. Success: {processed}, Failed: {failed}"
    )
    return processed, failed


if __name__ == "__main__":
    enrich_ip_locations()
