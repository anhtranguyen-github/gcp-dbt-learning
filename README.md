# gcp-dbt-learning

**Solution Project 5**.

## Solution Project 5 Setup

1. **Prerequisites**:
   - Ensure you have Python 3 installed.
   - Install Python dependencies:
     ```sh
     pip install -r requirements.txt
     ```
   - Ensure MongoDB is installed and running. Use the [setup.sh](http://_vscodecontentref_/0) script for MongoDB installation if needed.

2. **Import Data**:
   - Run the [import-data.sh](http://_vscodecontentref_/1) script to download and import raw data into MongoDB:
     ```sh
     bash import-data.sh
     ```

3. **Solution Project 5 Scripts**:
   - **Change Directory**:
     - Before running any scripts, change your working directory to the solution project folder:
       ```sh
       cd solution-prj5
       ```
   - **Install IP2Location Database**:
     - Execute the 0.install-ip2loc.sh script to download and install the IP2Location database:
       ```sh
       bash 0.install-ip2loc.sh
       ```
   - **Extract Distinct IPs**:
     - Run the 1.extract_distinct_ips.py script to aggregate and extract distinct IP addresses from the `summary` collection:
       ```sh
       python 1.extract_distinct_ips.py
       ```
   - **Enrich IP Locations**:
     - Execute the 2.ip-location-processing.py script to enrich the extracted IP addresses using the IP2Location database:
       ```sh
       python 2.ip-location-processing.py
       ```
   - **Initialize Product Names Collection**:
     - Run the 3.product-name-collection-init.py script to create and populate the `product_names` collection:
       ```sh
       python 3.product-name-collection-init.py
       ```
   - **Crawl Product Names**:
     - Execute the 4.crawl-product-name.py script to scrape product names from URLs and update the MongoDB collection.
     - If some entries fail to fetch, use 4.1.failed-handle.py to analyze and diagnose the failed records:
       ```sh
       python 4.crawl-product-name.py
       ```
       ```sh
       python 4.1.failed-handle.py
       ```
   - **Save Product Names to CSV**:
     - Finally, export the collected product names to a CSV file by running the 5.save-product-names-to-csv.py script:
       ```sh
       python 5.save-product-names-to-csv.py
       ```
   - **Data Profiling**:
     - Run the 6.documentaion-testing.py script to perform data profiling on your collections. This script collects metrics such as document counts, distinct field values, null/empty counts, and status distributions.
       ```sh
       python 6.data-profiling.py
       ```
     - The profiling output will be saved to:
       ```
       data_profiling_output.txt
       ```

4. **Logs**:



