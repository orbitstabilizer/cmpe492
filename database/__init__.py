"""
Database module for CMPE492 cryptocurrency analysis project
Provides PostgreSQL connection management and data operations
"""

from .db_init import get_database_client, init_db, get_db, close_db

__all__ = ["get_database_client", "init_db", "get_db", "close_db"]
