import requests
import os
import gzip
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

from database import ProcessedData, SessionLocal, get_session

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

def download_file() -> None:
        """Stream download with 100 MB progress logging."""
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
                    downloaded_size = 0
                    next_log_threshold = 100 * 1024 * 1024  # 100 MB in bytes
                    
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Log every 100 MB milestone
                            if downloaded_size >= next_log_threshold:
                                logged_mb = next_log_threshold // (1024 * 1024)
                                logger.info(f"Downloaded: {logged_mb} MB")
                                next_log_threshold += 100 * 1024 * 1024  # Next 100 MB

                    # Final log if remaining bytes < 100 MB
                    if downloaded_size > (next_log_threshold - 100 * 1024 * 1024):
                        final_mb = downloaded_size / (1024 * 1024)
                        logger.info(f"Download completed: {final_mb:.2f} MB")

            logger.info(f"File saved to: {TEMP_FILE}")

        except KeyboardInterrupt:
            logger.warning("Download interrupted by user (Ctrl+C).")
            if os.path.exists(TEMP_FILE):
                logger.info("Removing partial download...")
                os.remove(TEMP_FILE)
            exit(1)

        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            raise

def clean_and_insert_chunk(df_chunk):
    """Function to be executed in separate processes."""
    session = get_session()

    try:
        # --- Cleaning ---
        df = df_chunk.copy()
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        numeric_cols = [
            'current_price', 'price', 'platform_commission_rate',
            'product_commission_rate', 'bonus_commission_rate',
            'discount_percentage', 'promotion_price', 'seller_rating',
            'rating_avg_value'
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['number_of_reviews'] = pd.to_numeric(df['number_of_reviews'], errors='coerce').fillna(0).astype(int)
        df['is_free_shipping'] = df['is_free_shipping'].astype(bool)

        text_cols = [
            'product_name', 'description', 'seller_name', 'brand_name',
            'venture_category1_name_en', 'venture_category2_name_en',
            'venture_category3_name_en', 'venture_category_name_local',
            'availability'
        ]
        for col in text_cols:
            df[col] = df[col].str.strip()
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)

        url_cols = [
            'product_small_img', 'product_medium_img', 'product_big_img',
            'image_url_2', 'image_url_3', 'image_url_4', 'image_url_5',
            'product_url', 'seller_url', 'deeplink'
        ]
        for col in url_cols:
            df[col] = df[col].where(
                df[col].str.contains(r'^https?://', regex=True, na=False),
                np.nan
            )

        df.dropna(subset=['product_id'], inplace=True)
        df.reset_index(drop=True, inplace=True)

        # --- Insert to DB ---
        records = df.to_dict('records')
        session.bulk_insert_mappings(ProcessedData, records, render_nulls=True)
        session.commit()
        logger.info(f"Inserted {len(df)} records")
        session.close()
        return len(df)

    except Exception as e:
        logger.error(f"Error in worker process: {e}")
        session.rollback()
        session.close()
        return 0

def process_file():
    """Use multiprocessing to process and insert CSV chunks."""
    try:
        logger.info("Starting parallel CSV processing with multiprocessing")
        batch_size = 10000
        num_workers = max(cpu_count() - 1, 1)
        processed_total = 0

        with gzip.open(TEMP_FILE, 'rt') as f:
            chunk_iter = pd.read_csv(f, chunksize=batch_size, low_memory=False)

            with Pool(processes=num_workers) as pool:
                for i, count in enumerate(pool.imap(clean_and_insert_chunk, chunk_iter), start=1):
                    processed_total += count
                    logger.info(f"Batch #{i} processed. Total inserted: {processed_total}")

        logger.info(f"All done. Total records inserted: {processed_total}")

    except Exception as e:
        logger.critical(f"Multiprocessing pipeline failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    try:
        start_time= datetime.now()
        download_file()
        process_file()
        end_time = datetime.now()
        logger.info(f"Total processing time: {end_time - start_time}")
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}")
        exit(1)