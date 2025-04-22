import os
import logging
from pymongo import MongoClient
import pandas as pd
from google.cloud import storage

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("export_log.log"),
        logging.StreamHandler()
    ]
)

# --- Config ---
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "countly"
COLLECTIONS = ["distinct_ips", "product_names", "summary"]
EXPORT_PATH = "./data"
BATCH_SIZE = 100
GCS_BUCKET_NAME = "your-gcs-bucket-name"  # <-- Replace with actual GCS bucket name

# --- Mongo Connection ---
def connect_mongo():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        logging.info("✅ Connected to MongoDB")
        return db
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise

# --- Upload to GCS ---
def upload_to_gcs(local_path, blob_name):
    try:
        client = storage.Client()
        bucket = client.get_bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        logging.info(f"📤 Uploaded {blob_name} to GCS bucket {GCS_BUCKET_NAME}")
    except Exception as e:
        logging.error(f"❌ Failed to upload {blob_name} to GCS: {e}")

# --- Export collection ---
def export_collection_to_parquet(db, collection_name, test_mode=False, sample_size=10, upload_mode=True):
    os.makedirs(EXPORT_PATH, exist_ok=True)
    collection = db[collection_name]

    if test_mode:
        total_docs = min(sample_size, collection.count_documents({}))
        logging.info(f"[TEST MODE] {collection_name}: Exporting first {total_docs} documents")
        cursor = collection.find().limit(sample_size)
    else:
        total_docs = collection.count_documents({})
        logging.info(f"{collection_name}: {total_docs} documents found.")
        cursor = collection.find()

    batch_num = 0
    while True:
        batch = list(cursor.limit(BATCH_SIZE).skip(batch_num * BATCH_SIZE))
        if not batch:
            break

        df = pd.DataFrame(batch)

        if "_id" in df.columns:
            df.drop(columns=["_id"], inplace=True)

        # Convert all columns to string to avoid serialization issues
        df = df.astype(str)

        test_prefix = "test_" if test_mode else ""
        file_name = f"{test_prefix}{collection_name}_batch_{batch_num}.parquet"
        file_path = os.path.join(EXPORT_PATH, file_name)

        df.to_parquet(file_path, index=False, engine="pyarrow")
        logging.info(f"📁 Exported {len(df)} records to {file_path}")

        if upload_mode:
            upload_to_gcs(file_path, file_name)
        else:
            logging.info(f"🚫 Skipped upload for {file_name} (upload_mode=False)")

        if test_mode:
            break  # Only one batch in test mode

        batch_num += 1

# --- Master Export Function ---
def export_to_gcs(test_mode=True, sample_size=10, upload_mode=False):
    try:
        db = connect_mongo()
        for collection in COLLECTIONS:
            export_collection_to_parquet(
                db,
                collection_name=collection,
                test_mode=test_mode,
                sample_size=sample_size,
                upload_mode=upload_mode
            )
        logging.info("✅ Export completed successfully.")
    except Exception as e:
        logging.error(f"❌ Export failed: {e}")

# --- Run (sample test mode) ---
if __name__ == "__main__":
    # Export test data only (10 records), don't upload to GCS
    export_to_gcs(test_mode=True, sample_size=10, upload_mode=False)

    # 👇 For full export + GCS upload:
    # export_to_gcs(test_mode=False, upload_mode=True)
