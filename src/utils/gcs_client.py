from google.cloud import storage
from config.settings import BUCKET_NAME

def get_client() -> storage.Client:
    return storage.Client()

def get_bucket() -> storage.Bucket:
    return get_client().bucket(BUCKET_NAME)
