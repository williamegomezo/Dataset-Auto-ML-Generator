from utils.gcp_utility import gcpStorageUtility
import os

bucket_name = 'automl-testing-231805-vcm'
gcp_storage_utility = gcpStorageUtility(
    credentials_path='keys/api-key.json', bucket_name=bucket_name)

gcp_storage_utility.upload_file(
    file_path='generator.py', bucket_filename='hola/subido.py')
