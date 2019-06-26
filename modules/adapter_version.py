from abc import ABC

import boto3
from azure.storage.blob import BlockBlobService
from google.cloud import storage


class Cloud_adaptee():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def create_file(self):
        import random
        tmp = [random.random() for _ in range(100)]
        with open(self.file_name, "w") as file:
            for i in range(len(tmp)):
                file.write(str(tmp[i]) + "\n")

    def wrap_data(self):
        file = open(self.file_name, "r")
        data = file.read()
        list_of_numbers = data.split("\n")
        import json
        with open(self.file_name, "w") as outfile:
            json.dump(list_of_numbers, outfile)


class AWS_adaptee(Cloud_adaptee):
    def __init__(self, **kwargs ):
        Cloud_adaptee.__init__(self, **kwargs )
        self.s3 = boto3.client("s3")

    def upload_file(self):
        self.s3.upload_file(self.file_name, self.bucket_name, self.file_name_in_cloud)

    def download_file(self):
        self.s3.download_file(self.bucket_name, self.file_name_in_cloud, self.file_name_after_downloaded)

    def rename_file(self):
        self.s3.copy_object(Bucket=self.bucket_name, Key=self.new_file_name, CopySource=self.bucket_name + "/" + self.file_name)
        self.s3.delete_object(Bucket=self.bucket_name, Key=self.file_name)

    def move_file(self):
        self.s3.copy_object(Bucket=self.new_bucket_name, Key=self.file_name, CopySource=self.bucket_name + "/" + self.file_name)
        self.s3.delete_object(Bucket=self.bucket_name, Key=self.file_name)


class Azure_adaptee(Cloud_adaptee):
    def __init__(self, **kwargs ):
        Cloud_adaptee.__init__(self, **kwargs )
        self.block_blob_service = BlockBlobService()

    def upload_file(self):
        full_path_to_file = "/home/anniekey/Projects/cloud_pipeline/" + self.file_name
        self.block_blob_service.create_blob_from_path(self.bucket_name, self.file_name, full_path_to_file)

    def download_file(self):
        self.block_blob_service.get_blob_to_path(self.bucket_name, self.file_name_in_cloud, self.file_name_after_downloaded)

    def rename_file(self):
        file_name = "files/" + self.file_name
        blob_url = self.block_blob_service.make_blob_url(self.bucket_name, file_name)
        self.block_blob_service.copy_blob(self.bucket_name, self.new_file_name, blob_url)
        self.block_blob_service.delete_blob(self.bucket_name, self.file_name)

    def move_file(self):
        blob_url = self.block_blob_service.make_blob_url(self.bucket_name, self.file_name)
        self.block_blob_service.copy_blob(self.new_bucket_name, self.file_name, blob_url)
        self.block_blob_service.delete_blob(self.bucket_name, self.file_name)


class Google_adaptee(Cloud_adaptee):
    def __init__(self, **kwargs):
        Cloud_adaptee.__init__(self, **kwargs)
        self.storage_client = storage.Client()

    def upload_file(self):
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.file_name_in_cloud)
        blob.upload_from_filename(self.file_name)

    def download_file(self):
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.file_name_in_cloud)
        blob.download_to_filename(self.file_name_after_downloaded)

    def rename_file(self):
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.file_name)
        new_blob = bucket.rename_blob(blob, self.new_file_name)

    def move_file(self):
        source_bucket = self.storage_client.get_bucket(self.bucket_name)
        source_blob = source_bucket.blob(self.file_name)
        destination_bucket = self.storage_client.get_bucket(self.new_bucket_name)
        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, self.file_name)
        source_blob.delete()


class Target_interface(ABC):
    def create_file(self): pass
    def upload_file(self): pass
    def download_file(self): pass
    def wrap_data(self): pass
    def rename_file(self): pass
    def move_file(self): pass


class Adapter(Target_interface, AWS_adaptee, Azure_adaptee, Google_adaptee):
    def __init__(self, adaptee, **kwargs ):
        Cloud_adaptee.__init__(self, **kwargs )
        self.adaptee = adaptee

    def create_file(self):
        self.adaptee.create_file()

    def upload_file(self):
        self.adaptee.upload_file()

    def download_file(self):
        self.adaptee.download_file()

    def wrap_data(self):
        self.adaptee.wrap_data()

    def rename_file(self):
        self.adaptee.rename_file()

    def move_file(self):
        self.adaptee.move_file()


class Client:
    def __init__(self, adapter):
        self.adapter = adapter

    def create_file(self):
        self.adapter.create_file()

    def upload_file(self):
        self.adapter.upload_file()

    def download_file(self):
        self.adapter.download_file()

    def wrap_data(self):
        self.adapter.wrap_data()

    def rename_file(self):
        self.adapter.rename_file()

    def move_file(self):
        self.adapter.move_file()


def main():
    bucket_name = "testcloudanniekey"
    new_bucket_name = "testmovement"
    file_name = "/home/anniekey/Projects/cloud_pipeline/files/test.txt"
    file_name_in_cloud = "created_file.txt"
    new_file_name = "renamed_file.txt"
    file_name_after_downloaded = "/home/anniekey/Projects/cloud_pipeline/files/downloaded_file.txt"

    adaptee = AWS_adaptee(file_name=file_name,
                      bucket_name=bucket_name,
                      file_name_in_cloud=file_name_in_cloud,
                      file_name_after_downloaded=file_name_after_downloaded,
                      new_file_name=new_file_name,
                      new_bucket_name=new_bucket_name)
    adapter = Adapter(adaptee)
    client = Client(adapter)


if __name__ == "__main__":
    main()
