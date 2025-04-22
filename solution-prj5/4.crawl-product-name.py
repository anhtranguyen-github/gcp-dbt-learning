import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import threading
import os
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import importlib.util
from tqdm import tqdm

# Ensure logs directory exists
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure detailed logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{LOG_DIR}/product_scraper_{timestamp}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configure summary logger
summary_logger = logging.getLogger('summary')
summary_handler = logging.FileHandler(f'{LOG_DIR}/summary_{timestamp}.log')
summary_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
summary_logger.addHandler(summary_handler)
summary_logger.setLevel(logging.INFO)

# Configuration
BATCH_SIZE = 50
DELAY = 0.1
TIMEOUT = 10
MAX_WORKERS = 16
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9'
}

# MongoDB connection pool
mongo_client = None
mongo_lock = threading.Lock()

# Parser availability check
def check_parser_availability():
    """Check if lxml and html5lib are available"""
    parsers = {'lxml': False, 'html5lib': False}
    if importlib.util.find_spec('lxml'):
        parsers['lxml'] = True
        logger.info("lxml parser available")
    else:
        logger.warning("lxml parser not installed. Install with 'pip install lxml' for better performance.")
    if importlib.util.find_spec('html5lib'):
        parsers['html5lib'] = True
        logger.info("html5lib parser available")
    else:
        logger.warning("html5lib parser not installed. Install with 'pip install html5lib' for robust parsing.")
    return parsers

def get_mongo_collection():
    global mongo_client
    with mongo_lock:
        if mongo_client is None:
            mongo_client = MongoClient(
                'mongodb://localhost:27017/',
                maxPoolSize=50
            )
        return mongo_client['countly']['product_names']

def log_system_metrics():
    """Log CPU and memory usage"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    logger.info(f"System Metrics: CPU Usage: {cpu_percent}% | Memory Usage: {memory.percent}% ({memory.used/1024**3:.2f} GB)")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    after=lambda retry_state: logger.debug(f"Retry {retry_state.attempt_number} for {retry_state.args[1]}")
)
def scrape_product_name(url, product_id):
    """Scrape product name for a single URL with retry logic"""
    try:
        logger.debug(f"Scraping: {product_id} - {url}")
        start_time = time.time()
        
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        
        parser_counts = {'lxml': 0, 'html5lib': 0, 'html.parser': 0}
        parser_used = None
        
        # Try parsers in order of preference
        for parser in ['lxml', 'html5lib', 'html.parser']:
            try:
                soup = BeautifulSoup(response.text, parser)
                parser_used = parser
                parser_counts[parser] += 1
                break
            except ValueError as e:
                logger.warning(f"{parser} parser unavailable for {product_id}: {str(e)}, trying next parser")
                continue
        
        if not parser_used:
            logger.error(f"No parsers available for {product_id}: {url}")
            return None, False, parser_counts
        
        logger.debug(f"Using {parser_used} parser for {product_id}")
        
        selectors = [
            {'name': 'h1', 'class': 'product-name'},
            {'class': 'product-title'},
            {'class': 'product_title'},
            {'name': 'h1'}
        ]
        
        for selector in selectors:
            element = soup.find(**selector) if isinstance(selector, dict) else soup.find(selector)
            if element:
                product_name = element.get_text(strip=True)
                if product_name:
                    logger.info(f"Found name for {product_id} using {selector}: {product_name} (HTTP {response.status_code}, {parser_used})")
                    return product_name, True, parser_counts
        logger.warning(f"No product name found for {product_id}: {url} (HTTP {response.status_code}, {parser_used})")
        return None, False, parser_counts
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request failed for {product_id}: {url} - {str(e)}")
        return None, False, {'lxml': 0, 'html5lib': 0, 'html.parser': 0}
    except Exception as e:
        logger.error(f"Unexpected error for {product_id}: {url} - {str(e)}")
        return None, False, {'lxml': 0, 'html5lib': 0, 'html.parser': 0}

def process_batch(docs):
    """Process a batch of documents in parallel"""
    operations = []
    succeeded = 0
    failed = 0
    parser_counts = {'lxml': 0, 'html5lib': 0, 'html.parser': 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_doc = {
            executor.submit(scrape_product_name, doc['current_url'], doc['product_id']): doc
            for doc in docs
        }
        
        for future in as_completed(future_to_doc):
            doc = future_to_doc[future]
            try:
                product_name, success, doc_parser_counts = future.result()
                for parser, count in doc_parser_counts.items():
                    parser_counts[parser] += count
                
                retry_count = doc.get('retry_count', 0) + 1
                if success:
                    operations.append(
                        UpdateOne(
                            {'_id': doc['_id']},
                            {'$set': {
                                'product_name': product_name,
                                'status': 'processed',
                                'retry_count': retry_count
                            }}
                        )
                    )
                    logger.info(f"Updated {doc['product_id']} to status: processed (Retries: {retry_count})")
                    succeeded += 1
                else:
                    operations.append(
                        UpdateOne(
                            {'_id': doc['_id']},
                            {'$set': {
                                'status': 'failed',
                                'retry_count': retry_count
                            }}
                        )
                    )
                    logger.info(f"Updated {doc['product_id']} to status: failed (Retries: {retry_count})")
                    failed += 1
            except Exception as e:
                logger.error(f"Error processing {doc['product_id']}: {str(e)}")
                failed += 1
    
    return operations, succeeded, failed, parser_counts

def update_all_product_names():
    """Update all product names with parallel processing and progress bar"""
    try:
        logger.info("=== Starting product name update ===")
        summary_logger.info("Starting product name update")
        start_time = time.time()
        
        # Check parser availability
        parser_availability = check_parser_availability()
        summary_logger.info(f"Parser availability: {parser_availability}")
        
        collection = get_mongo_collection()
        
        total_to_process = collection.count_documents({
            'status': 'pending',
            'current_url': {'$exists': True},
            '$or': [{'retry_count': {'$exists': False}}, {'retry_count': {'$lt': 3}}]
        })
        logger.info(f"Found {total_to_process} documents with status 'pending' and retry_count < 3")
        summary_logger.info(f"Documents to process: {total_to_process}")
        
        if total_to_process == 0:
            logger.info("No documents need updating")
            summary_logger.info("No documents to process")
            return
        
        cursor = collection.find({
            'status': 'pending',
            'current_url': {'$exists': True},
            '$or': [{'retry_count': {'$exists': False}}, {'retry_count': {'$lt': 3}}]
        }).batch_size(BATCH_SIZE)
        
        batch = []
        operations = []
        processed = 0
        succeeded = 0
        failed = 0
        total_parser_counts = {'lxml': 0, 'html5lib': 0, 'html.parser': 0}
        
        # Initialize tqdm progress bar
        with tqdm(total=total_to_process, desc="Processing documents", unit="doc") as pbar:
            for doc in cursor:
                batch.append(doc)
                processed += 1
                
                if len(batch) >= BATCH_SIZE:
                    batch_ops, batch_succeeded, batch_failed, batch_parser_counts = process_batch(batch)
                    operations.extend(batch_ops)
                    succeeded += batch_succeeded
                    failed += batch_failed
                    for parser, count in batch_parser_counts.items():
                        total_parser_counts[parser] += count
                    
                    if operations:
                        result = collection.bulk_write(operations, ordered=False)
                        logger.debug(f"Updated {result.modified_count} documents in batch")
                        summary_logger.info(f"Batch update: {len(operations)} operations, {result.modified_count} modified")
                        operations = []
                    
                    # Update progress bar
                    pbar.update(len(batch))
                    log_system_metrics()
                    
                    batch = []
                    time.sleep(DELAY)
            
            # Process final batch
            if batch:
                batch_ops, batch_succeeded, batch_failed, batch_parser_counts = process_batch(batch)
                operations.extend(batch_ops)
                succeeded += batch_succeeded
                failed += batch_failed
                for parser, count in batch_parser_counts.items():
                    total_parser_counts[parser] += count
                
                if operations:
                    result = collection.bulk_write(operations, ordered=False)
                    logger.debug(f"Updated final {result.modified_count} documents")
                    summary_logger.info(f"Final batch update: {len(operations)} operations, {result.modified_count} modified")
                
                # Update progress bar for final batch
                pbar.update(len(batch))
        
        duration = time.time() - start_time
        logger.info("=== Update completed ===")
        logger.info(f"Total processed: {processed}")
        logger.info(f"Successfully updated: {succeeded}")
        logger.info(f"Failed to update: {failed}")
        logger.info(f"Success rate: {succeeded/max(processed,1)*100:.1f}%")
        logger.info(f"Total duration: {duration:.2f} seconds")
        logger.info(f"Average speed: {processed/max(duration,1):.2f} docs/second")
        logger.info(f"Parser usage: {total_parser_counts}")
        log_system_metrics()
        
        summary_logger.info(f"Update completed: {processed} processed, {succeeded} succeeded, {failed} failed")
        summary_logger.info(f"Success rate: {succeeded/max(processed,1)*100:.1f}%")
        summary_logger.info(f"Duration: {duration:.2f} seconds, Speed: {processed/max(duration,1):.2f} docs/sec")
        summary_logger.info(f"Parser usage: {total_parser_counts}")
        
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        summary_logger.critical(f"Fatal error: {str(e)}")
    finally:
        global mongo_client
        with mongo_lock:
            if mongo_client is not None:
                mongo_client.close()
                mongo_client = None
                logger.info("MongoDB connection closed")
                summary_logger.info("MongoDB connection closed")

def test_scrape_single_product(url):
    """Test scraping for a single product URL"""
    try:
        logger.info(f"Testing URL: {url}")
        summary_logger.info(f"Testing single URL: {url}")
        
        product_name, success, parser_counts = scrape_product_name(url, "test_product")
        
        if success:
            logger.info(f"Success! Found product name: {product_name}")
            summary_logger.info(f"Test success: Found {product_name}")
            return True
        else:
            logger.warning("Could not find product name in the page")
            summary_logger.warning("Test failed: No product name found")
            return False
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        summary_logger.error(f"Test error: {str(e)}")
        return False

if __name__ == '__main__':
    test_url = "https://www.glamira.pl/glamira-pendant-amaryllis.html?alloy=yellow-375"
    
    logger.info("Starting single product test...")
    success = test_scrape_single_product(test_url)
    
    if success:
        logger.info("Test successful! Running full scraper...")
        summary_logger.info("Test successful, starting full scraper")
        update_all_product_names()
    else:
        logger.error("Test failed. Check the URL or scraping logic before running full scraper.")
        summary_logger.error("Test failed, scraper not started")