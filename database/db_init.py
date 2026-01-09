"""
Database initialization utilities
"""

import logging
import os
from typing import Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def get_database_client(**kwargs):
    """
    Get PostgreSQL database client
    
    Args:
        **kwargs: Override connection parameters
    
    Returns:
        Database client instance
    
    Usage:
        # Use default connection from environment
        db = get_database_client()
        
        # Custom connection
        db = get_database_client(host='remote.db.com', port=5433)
    """
    from .db_client import CryptoExchangeDB
    
    # Use environment variables or kwargs
    connection_params = {
        'host': kwargs.get('host') or os.getenv('DB_HOST', 'localhost'),
        'port': kwargs.get('port') or int(os.getenv('DB_PORT', 5432)),
        'database': kwargs.get('database') or os.getenv('DB_NAME', 'crypto_exchange'),
        'user': kwargs.get('user') or os.getenv('DB_USER', 'cmpe492'),
        'password': kwargs.get('password') or os.getenv('DB_PASSWORD', 'password123'),
    }
    
    db = CryptoExchangeDB(**connection_params)
    if db.health_check():
        logger.info("✅ Connected to PostgreSQL database")
        return db
    else:
        raise ConnectionError("Failed to connect to PostgreSQL database")


# Global database instance
_db_instance = None


def init_db(**kwargs):
    """Initialize global database instance"""
    global _db_instance
    _db_instance = get_database_client(**kwargs)
    return _db_instance


def get_db():
    """Get global database instance"""
    global _db_instance
    if _db_instance is None:
        _db_instance = get_database_client()
    return _db_instance


def close_db():
    """Close global database instance"""
    global _db_instance
    if _db_instance:
        _db_instance.close()
        _db_instance = None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    try:
        db = get_database_client()
        print(f"✅ Database health check: {db.health_check()}")
        db.close()
    except Exception as e:
        print(f"❌ Error: {e}")
