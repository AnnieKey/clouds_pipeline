from google.cloud import storage


def create_file(file_name, *args, **kwargs):
    import random
    tmp=[random.random() for _ in range(100)]
    with open(file_name, 'w') as f:
        for i in range(len(tmp)):
            f.write(str(i) + ":")
            f.write(str(tmp[i]))
            f.write(' ')


def upload_file(bucket_name, file_name_local, file_name_in_cloud):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name_in_cloud)
    blob.upload_from_filename(file_name_local)


def download_file(bucket_name, file_name_in_cloud, downloaded_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name_in_cloud)
    blob.download_to_filename(downloaded_file_name)


def wrap_data(file_name, *args, **kwargs):
    import json
    file =open(file_name, "r")
    data=file.read()
    data_json = json.dumps(data, separators=(":",""))
    return data_json


def rename_file(bucket_name, file_name, new_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    new_blob = bucket.rename_blob(blob, new_file_name)


def move_file(file_name, bucket_name, new_bucket_name, *args, **kwargs):
    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(file_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)
    new_blob = source_bucket.copy_blob(source_blob, destination_bucket, file_name)
    source_blob.delete()

