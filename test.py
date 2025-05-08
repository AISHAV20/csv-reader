from multiprocessing import Pool, cpu_count
import pandas as pd
import gzip
from database import ProcessedData, SessionLocal, get_session
from datetime import datetime
import numpy as np
import logging

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
        # processor.download_file()
        process_file()
        end_time = datetime.now()
        logger.info(f"Total processing time: {end_time - start_time}")
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}")
        exit(1)