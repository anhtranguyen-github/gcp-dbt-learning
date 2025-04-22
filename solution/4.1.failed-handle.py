import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import logging
from datetime import datetime
import os
import csv
import time

# Ensure logs directory exists
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{LOG_DIR}/failed_error_analyzer_{timestamp}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
TIMEOUT = 10
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9'
}
OUTPUT_FILE = f'failed_errors_{timestamp}.csv'

def get_mongo_collection():
    """Connect to MongoDB and return the product_names collection"""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        return client['countly']['product_names'], client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def diagnose_error(url, product_id):
    """Diagnose the error for a single URL"""
    try:
        logger.info(f"Diagnosing: {product_id} - {url}")
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Try lxml parser, fall back to html.parser
        try:
            soup = BeautifulSoup(response.text, 'lxml')
        except Exception:
            soup = BeautifulSoup(response.text, 'html.parser')
            logger.warning(f"lxml unavailable for {product_id}, using html.parser")
        
        selectors = [
            {'name': 'h1', 'class': 'product-name'},
            {'class': 'product-title'},
            {'class': 'product_title'},
            {'name': 'h1'}
        ]
        
        for selector in selectors:
            element = soup.find(**selector) if isinstance(selector, dict) else soup.find(selector)
            if element and element.get_text(strip=True):
                logger.info(f"Found name for {product_id}, no error")
                return "No error"
        return "No selectors matched"
        
    except requests.exceptions.HTTPError as e:
        return f"HTTP {response.status_code}"
    except requests.exceptions.Timeout:
        return "Timeout"
    except requests.exceptions.ConnectionError:
        return "Connection error"
    except requests.exceptions.RequestException as e:
        return f"Request error: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"

def analyze_failed_documents():
    """Analyze failed documents and output product_id, url, error type"""
    client = None
    try:
        logger.info("=== Starting analysis of failed documents ===")
        start_time = time.time()
        
        collection, client = get_mongo_collection()
        
        total_failed = collection.count_documents({'status': 'failed'})
        logger.info(f"Found {total_failed} documents with status 'failed'")
        
        if total_failed == 0:
            logger.info("No failed documents to analyze")
            return
        
        cursor = collection.find({'status': 'failed'})
        
        # Prepare CSV output
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['product_id', 'url', 'error type'])
            
            for doc in cursor:
                product_id = doc['product_id']
                url = doc.get('current_url', '')
                
                if not url:
                    error_type = "Missing URL"
                    logger.warning(f"No URL for {product_id}")
                else:
                    error_type = diagnose_error(url, product_id)
                    logger.info(f"Error for {product_id}: {error_type}")
                
                writer.writerow([product_id, url, error_type])
        
        duration = time.time() - start_time
        logger.info("=== Analysis completed ===")
        logger.info(f"Total analyzed: {total_failed}")
        logger.info(f"Output written to: {OUTPUT_FILE}")
        logger.info(f"Total duration: {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Fatal error during analysis: {str(e)}")
    finally:
        if client is not None:
            client.close()
            logger.info("MongoDB connection closed")

if __name__ == '__main__':
    analyze_failed_documents()