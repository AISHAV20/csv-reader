# data_pipeline.py
import os
import gzip
import logging
import uuid
import pandas as pd
import numpy as np
import requests
from database import SessionLocal, ProcessedData
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

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

    def __del__(self):
        self.session.close()

    def download_file(self) -> None:
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
            self.session.close()
            exit(1)

        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            raise

    def clean_and_transform(self, df: pd.DataFrame):
        """Perform comprehensive data cleaning and transformation."""
        try:
            # Create a copy to avoid SettingWithCopyWarning
            df = df.copy()
            
            # ---- 1. Initial Cleaning ----
            # Replace empty strings and whitespace-only strings with NaN
            df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
            
            # ---- 2. Type Conversion ----
            # Numeric columns with coercion
            numeric_cols = [
                'current_price', 'price', 'platform_commission_rate',
                'product_commission_rate', 'bonus_commission_rate',
                'discount_percentage', 'promotion_price', 'seller_rating',
                'rating_avg_value'
            ]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Integer columns
            df['number_of_reviews'] = pd.to_numeric(
                df['number_of_reviews'], 
                errors='coerce'
            ).fillna(0).astype(int)
            
            # Boolean column
            df['is_free_shipping'] = df['is_free_shipping'].astype(bool)
            
            # ---- 3. Text Normalization ----
            text_cols = [
                'product_name', 'description', 'seller_name', 'brand_name',
                'venture_category1_name_en', 'venture_category2_name_en',
                'venture_category3_name_en', 'venture_category_name_local',
                'sku_id', 'business_type', 'business_area', 'availability'
            ]
            for col in text_cols:
                df[col] = df[col].str.strip()
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
            
            # ---- 4. URL Validation ----
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

            # Remove rows missing critical fields
            df.dropna(subset=['product_id'], inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            return df
        
        except Exception as e:
            logger.error(f"Data transformation error: {str(e)}")
            return None

    def _save_debug_data(self, df: pd.DataFrame, error_type: str) -> None:
        """Save problematic data for debugging."""
        try:
            debug_dir = "debug_data"
            os.makedirs(debug_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{debug_dir}/error_{error_type}_{timestamp}.csv"
            
            # Ensure we have a DataFrame (not TextFileReader)
            if hasattr(df, 'to_csv'):
                df.to_csv(filename, index=False)
                logger.warning(f"Saved debug data to {filename}")
            else:
                logger.error(f"Cannot save debug data - invalid object type: {type(df)}")
        except Exception as e:
            logger.error(f"Failed to save debug data: {str(e)}")

    def process_file(self) -> None:
        """Process CSV in strict 10,000-record batches with immediate saving."""
        try:
            logger.info("Starting batch file processing")
            processed_count = 0
            batch_size = 10000  # Both read and insert batch size

            with gzip.open(TEMP_FILE, 'rt') as f , SessionLocal() as session:
                chunk_iter = pd.read_csv(f, chunksize=batch_size, low_memory=False)

                for i, df in enumerate(chunk_iter, start=1):
                    logger.info(f"Processing batch #{i}, records: {len(df)}")
                    df_clean = self.clean_and_transform(df)

                    if df_clean is None:
                        logger.warning(f"Batch #{i} skipped due to cleaning error")
                        continue

                    # 3. Convert to records and insert
                    records = df_clean.to_dict('records')
                    try:
                        session.bulk_insert_mappings(
                            ProcessedData,
                            records,
                            render_nulls=True
                        )
                        session.commit()
                        
                        processed_count += len(df_clean)
                        logger.info(
                            f"Saved batch with {len(df_clean)} records | "
                            f"Total saved: {processed_count}"
                        )
                    
                    except Exception as e:
                        session.rollback()
                        logger.error(f"Failed to save batch: {str(e)}")
                        raise

            logger.info(f"Finished processing. Total records saved: {processed_count}")

        except Exception as e:
            logger.critical(f"File processing failed: {str(e)}", exc_info=True)
            raise

if __name__ == '__main__':
    processor = DataProcessor()
    try:
        # processor.download_file()
        processor.process_file()
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}")
        exit(1)
    finally:
        processor.session.close()
        logger.info("Database session closed")