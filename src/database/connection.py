import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

client = MongoClient(MONGO_URI)

# Existing DB instance (keep this)
db = client[MONGO_DB]


# ✅ ADD THIS FUNCTION (IMPORTANT)
def get_db():
    return db