# database.py
import os
import logging
import uuid

from sqlalchemy import Column, String, Float, Boolean, DateTime, Index, Integer, Text, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()


class ProcessedData(Base):
    __tablename__ = 'processed_data'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True)
    
    product_id = Column(String(50), nullable=False)
    product_name = Column(String(255), nullable=False)
    sku_id = Column(String(50))
    description = Column(Text)
    
    current_price = Column(Float)
    price = Column(Float)
    platform_commission_rate = Column(Float)
    product_commission_rate = Column(Float)
    bonus_commission_rate = Column(Float)
    discount_percentage = Column(Float)
    promotion_price = Column(Float)
    
    product_small_img = Column(String(500))
    product_medium_img = Column(String(500))
    product_big_img = Column(String(500))
    image_url_2 = Column(String(500))
    image_url_4 = Column(String(500))
    image_url_5 = Column(String(500))
    image_url_3 = Column(String(500))
    product_url = Column(String(500))
    seller_url = Column(String(500))
    deeplink = Column(String(500))
    
    business_type = Column(String(50))
    business_area = Column(String(50))
    seller_name = Column(String(255))
    brand_name = Column(String(255))
    
    venture_category1_name_en = Column(String(100))
    venture_category2_name_en = Column(String(100))
    venture_category3_name_en = Column(String(100))
    venture_category_name_local = Column(String(100))
    
    seller_rating = Column(Float)
    rating_avg_value = Column(Float)
    number_of_reviews = Column(Integer)
    
    is_free_shipping = Column(Boolean)
    availability = Column(String(50))
    
    # Indexes
    __table_args__ = (
        Index('idx_product_id', 'product_id'),
        Index('idx_seller', 'seller_name'),
        Index('idx_category1', 'venture_category1_name_en'),
        Index('idx_price_range', 'current_price'),
    )

def get_database_url() -> str:
    """Get database URL from environment variables"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not found in environment variables")
        raise ValueError("Database URL not configured")
    return db_url

def create_engine_pool():
    """Create and configure SQLAlchemy engine with connection pooling"""
    return create_engine(
        get_database_url(),
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=10,
        pool_recycle=3600
    )

def create_session_factory(engine):
    """Create scoped session factory"""
    return scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )
    )

def create_tables(engine):
    """Create database tables"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

# Initialize database components
engine = create_engine_pool()
SessionLocal = create_session_factory(engine)
create_tables(engine)