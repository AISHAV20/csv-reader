# data_pipeline.py
import os
import gzip
import logging
import uuid
import pandas as pd
import requests
from typing import Generator
from datetime import datetime
from database import SessionLocal, ProcessedData  # From previous database.py

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CHUNK_SIZE = 5000  # Adjust based on memory constraints
CSV_URL = 'https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz'
TEMP_FILE = 'temp.csv.gz'

CHUNK_SIZE_DOWNLOAD = 1024 * 1024  # 1MB chunk
TIMEOUT = 30

class DataProcessor:
    def __init__(self):
        self.session = SessionLocal()
        self.total_processed = 0
        self.failed_chunks = 0

    def __del__(self):
        self.session.close()

    def download_file(self) -> None:
        """Stream download without timeout; safely overwrites if file exists."""
        try:
            logger.info(f"Starting download from {CSV_URL}")

            if os.path.exists(TEMP_FILE):
                os.remove(TEMP_FILE)
                logger.info(f"Existing temp file '{TEMP_FILE}' removed.")

            with requests.get(CSV_URL, stream=True, timeout=None) as response:
                response.raise_for_status()

                total_size = response.headers.get('content-length')
                if total_size:
                    logger.info(f"File size: {int(total_size) / 1024 / 1024:.2f} MB")

                with open(TEMP_FILE, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

            logger.info(f"Download completed. Size: {os.path.getsize(TEMP_FILE) / 1024 / 1024:.2f} MB")

        except KeyboardInterrupt:
            logger.warning("Download interrupted by user (Ctrl+C).")
            if os.path.exists(TEMP_FILE):
                logger.info("Removing partial download...")
                os.remove(TEMP_FILE)
            processor.session.close()  # or .remove() if scoped session
            exit(1)

        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            raise

    def process_chunk(self, df: pd.DataFrame) -> list:
        """Clean and transform data chunk"""
        try:
            # Generate UUIDs
            df['id'] = [uuid.uuid4() for _ in range(len(df))]
            
            # Rename columns with typos
            df.rename(columns={'promotin_price': 'promotion_price'}, inplace=True)

            # Convert numeric columns
            numeric_cols = [
                'current_price', 'price', 'platform_commission_rate',
                'product_commission_rate', 'bonus_commission_rate',
                'discount_percentage', 'promotion_price'
            ]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

            # Convert boolean column
            df['is_free_shipping'] = df['is_free_shipping'].astype(bool)

            # Clean text columns
            text_cols = ['product_name', 'brand_name', 'description']
            df[text_cols] = df[text_cols].fillna('').apply(lambda x: x.str.strip())

            # Convert URLs to nullable strings
            url_cols = [col for col in df.columns if '_url' in col or 'link' in col]
            df[url_cols] = df[url_cols].where(df[url_cols].str.startswith('http', na=False), None)

            # Convert category names
            category_cols = [
                'venture_category1_name_en',
                'venture_category2_name_en',
                'venture_category3_name_en'
            ]
            df[category_cols] = df[category_cols].fillna('Uncategorized').apply(lambda x: x.str.title())

            return df.to_dict('records')

        except Exception as e:
            logger.error(f"Chunk processing failed: {str(e)}")
            raise

    def insert_batch(self, batch: list) -> None:
        """Insert batch with transaction management"""
        try:
            self.session.bulk_insert_mappings(ProcessedData, batch)
            self.session.commit()
            self.total_processed += len(batch)
        except Exception as e:
            self.session.rollback()
            self.failed_chunks += 1
            logger.error(f"Batch insert failed: {str(e)}")
            raise

    def process_file(self) -> None:
        """Process CSV in chunks with memory management"""
        try:
            logger.info("Starting file processing")
            with gzip.open(TEMP_FILE, 'rt') as f:
                reader = pd.read_csv(
                    f,
                    chunksize=CHUNK_SIZE,
                    dtype={
                        'product_id': 'string',
                        'sku_id': 'string',
                        'business_type': 'category'
                    },
                    parse_dates=['timestamp'],
                    true_values=['Yes', 'Y'],
                    false_values=['No', 'N'],
                    on_bad_lines='warn'
                )

                for chunk_number, chunk in enumerate(reader, 1):
                    try:
                        processed = self.process_chunk(chunk)
                        self.insert_batch(processed)
                        logger.info(f"Processed chunk {chunk_number} | Total: {self.total_processed}")
                        
                        # Explicit memory cleanup
                        del processed
                        if chunk_number % 10 == 0:
                            pd.DataFrame().empty  # Force garbage collection
                        
                    except Exception as e:
                        logger.error(f"Failed processing chunk {chunk_number}: {str(e)}")
                        continue

            logger.info(f"Processing complete. Total records: {self.total_processed}")
            if self.failed_chunks > 0:
                logger.warning(f"Failed chunks: {self.failed_chunks}")

        except Exception as e:
            logger.error(f"File processing failed: {str(e)}")
            raise
        finally:
            if os.path.exists(TEMP_FILE):
                os.remove(TEMP_FILE)
                logger.info("Temporary file cleaned up")

if __name__ == '__main__':
    processor = DataProcessor()
    try:
        processor.download_file()
        # processor.process_file()
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}")
        exit(1)
    finally:
        processor.session.remove()
        logger.info("Database session closed")