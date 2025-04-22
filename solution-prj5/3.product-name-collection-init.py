import logging
from pymongo import MongoClient
from pymongo import UpdateOne
import sys
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/product_name_collection_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000  # Process documents in batches of 5000

def create_product_name_collection():
    """Create fresh product_names collection using aggregation for distinct product IDs"""
    
    try:
        # MongoDB connection
        try:
            client = MongoClient('mongodb://localhost:27017/', connectTimeoutMS=30000)
            client.admin.command('ping')
            db = client['countly']
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

        # Clean up old collection if exists
        if 'product_names' in db.list_collection_names():
            db['product_names'].drop()
            logger.info("Dropped existing product_names collection")

        # Collections setup
        source_collection = db['summary']
        target_collection = db['product_names']

        # Define the aggregation pipeline
        pipeline = [
            {
                '$match': {
                    'collection': {
                        '$in': [
                            'view_product_detail',
                            'select_product_option', 
                            'select_product_option_quality'
                        ]
                    },
                    'product_id': {'$exists': True, '$ne': ''}
                }
            },
            {
                '$group': {
                    '_id': '$product_id',
                    'current_url': {'$first': '$current_url'}
                }
            },
            {
                '$project': {
                    'product_id': '$_id',
                    'current_url': 1,
                    '_id': 0
                }
            }
        ]

        # Get estimated count for progress bar
        estimated_count = source_collection.count_documents({
            'collection': {
                '$in': [
                    'view_product_detail',
                    'select_product_option', 
                    'select_product_option_quality'
                ]
            },
            'product_id': {'$exists': True, '$ne': ''}
        })
        
        logger.info(f"Processing estimated {estimated_count} documents to find distinct product IDs")

        # Execute aggregation with cursor
        cursor = source_collection.aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=BATCH_SIZE
        )

        # Initialize batch processing
        operations = []
        processed_count = 0

        with tqdm(desc="Processing distinct products", unit="product") as pbar:
            for doc in cursor:
                try:
                    product_id = doc['product_id']
                    current_url = doc.get('current_url', '')
                    
                    if not product_id:
                        continue

                    # Prepare bulk operation
                    operations.append(
                        UpdateOne(
                            {'product_id': product_id},
                            {
                                '$setOnInsert': {
                                    'product_id': product_id,
                                    'current_url': current_url,
                                    'product_name': None,
                                    'status': 'pending'
                                }
                            },
                            upsert=True
                        )
                    )

                    # Execute batch when reaching batch size
                    if len(operations) == BATCH_SIZE:
                        result = target_collection.bulk_write(operations, ordered=False)
                        processed_count += result.upserted_count
                        operations = []
                        pbar.update(BATCH_SIZE)

                except Exception as e:
                    logger.error(f"Error processing document {doc.get('_id')}: {str(e)}")
                    continue

            # Process remaining operations in final batch
            if operations:
                result = target_collection.bulk_write(operations, ordered=False)
                processed_count += result.upserted_count
                pbar.update(len(operations))

        # Get actual distinct count (more accurate than processed_count due to upserts)
        distinct_count = target_collection.count_documents({})
        logger.info(f"Successfully processed {distinct_count} distinct products")
        logger.info(f"Final collection count: {distinct_count}")

        # Create index on product_id
        target_collection.create_index('product_id', unique=True)
        logger.info("Created unique index on product_id")

    except Exception as e:
        logger.critical(f"Script failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

if __name__ == '__main__':
    create_product_name_collection()