"""Shared settings. Everything secret comes from the .env file, never from code."""
import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

DB_NAME = os.getenv("MONGODB_DB", "sentinel")
CUSTOMERS = "customers"
TRANSACTIONS = "transactions"
DATA_DIR = os.getenv("DATA_DIR", "supabase_db_kit/data/tables")
TARGET_TXNS = int(os.getenv("TARGET_TXNS", "20000"))


def get_client() -> MongoClient:
    uri = os.getenv("MONGODB_URI")
    if not uri:
        sys.exit("MONGODB_URI is not set. Copy .env.example to .env and fill it in.")
    # tz_aware=True so datetimes come back with a timezone, same as we stored them
    return MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=15000)


def get_db():
    return get_client()[DB_NAME]
