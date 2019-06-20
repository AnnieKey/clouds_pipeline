from azure.storage.blob import BlockBlobService


def create_file(file_name, *args, **kwargs):
    import random
    tmp=[random.random() for _ in range(100)]
    with open(file_name, 'w') as f:
        for i in range(len(tmp)):
            f.write(str(i) + ":")
            f.write(str(tmp[i]))
            f.write(' ')


def upload_file(file_name, bucket_name, file_name_in_cloud):
    full_path_to_file='/home/anniekey/Projects/cloud_pipeline/' + file_name
    block_blob_service.create_blob_from_path(bucket_name, file_name, full_path_to_file)

def download_file(bucket_name, file_name_in_cloud, downloaded_file_name):
    block_blob_service.get_blob_to_path(bucket_name, file_name_in_cloud, downloaded_file_name)


def wrap_data(file_name, *args, **kwargs):
    import json
    file =open(file_name, "r")
    data=file.read()
    data_json = json.dumps(data, separators=(":",""))
    return data_json


def rename_file(file_name, bucket_name, new_file_name, *args, **kwargs):
    file_name='files/'+file_name
    blob_url = block_blob_service.make_blob_url(bucket_name, file_name)
    block_blob_service.copy_blob(bucket_name, new_file_name, blob_url)
    block_blob_service.delete_blob(bucket_name, file_name)


def move_file(file_name, bucket_name, new_bucket_name, *args, **kwargs):
    blob_url = block_blob_service.make_blob_url(bucket_name, file_name)
    block_blob_service.copy_blob(new_bucket_name, file_name, blob_url)
    block_blob_service.delete_blob(bucket_name, file_name)
