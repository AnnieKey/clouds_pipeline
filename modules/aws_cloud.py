import boto3

s3 = boto3.client('s3')

def create_file(file_name, *args, **kwargs):
    import random
    tmp=[random.random() for _ in range(100)]
    with open(file_name, 'w') as f:
        for i in range(len(tmp)):
            f.write(str(i) + ":")
            f.write(str(tmp[i]))
            f.write(' ')


def upload_file(file_name, bucket_name, file_name_in_s3):
    s3.upload_file(file_name, bucket_name, file_name_in_s3)


def download_file(bucket_name, file_name_in_cloud, file_name_after_downloaded):
    s3.download_file(bucket_name, file_name_in_cloud, file_name_after_downloaded)


def wrap_data(file_name, *args, **kwargs):
    import json
    file =open(file_name, "r")
    data=file.read()
    data_json = json.dumps(data, separators=(":",""))
    return data_json


def rename_file(file_name, bucket_name, new_file_name, *args, **kwargs):
    s3.copy_object(Bucket=bucket_name, Key=new_file_name, CopySource=bucket_name+'/'+file_name)
    s3.delete_object(Bucket=bucket_name, Key=file_name)


def move_file(file_name, bucket_name, new_bucket_name, *args, **kwargs):
    s3.copy_object(Bucket=new_bucket_name, Key=file_name, CopySource=bucket_name+'/'+file_name)
    s3.delete_object(Bucket=bucket_name, Key=file_name)

