from google.oauth2 import service_account
from google.cloud import storage
import os


class gcpStorageUtility():
    def __init__(self, credentials_path, bucket_name):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)

    def upload_file(self, file_path, bucket_filename):
        blob = self.bucket.blob(bucket_filename)
        with open(file_path, "rb") as my_file:
            blob.upload_from_file(my_file)
