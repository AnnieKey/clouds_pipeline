from unittest import TestCase

import os
import boto3
import botocore

from modules import adapter_version


class Cloud_adaptee_test(TestCase):
    def setUp(self) -> None:
        self.file_name = "/home/anniekey/Projects/cloud_pipeline/files/test1.txt"
        self.cloud_adaptee = adapter_version.Cloud_adaptee(
            file_name=self.file_name)

    def test_create_file(self):
        self.cloud_adaptee.create_file()

        #check the name of file
        self.assertEqual(self.file_name, self.cloud_adaptee.file_name)

        #check if file exists
        self.assertTrue(os.path.exists(self.file_name))

        # check if it is a file
        self.assertTrue(os.path.isfile(self.file_name))

    def test_wrap_data(self):
        self.cloud_adaptee.wrap_data()

        import json
        try:
            with open(self.file_name, "r") as read_file:
                data = json.load(read_file)
            result = True
        except ValueError:
            result = False
        self.assertTrue(result)

class AWS_adaptee_test(TestCase):
    def setUp(self) -> None:
        self.s3 = boto3.resource('s3')
        self.file_name = "/home/anniekey/Projects/cloud_pipeline/files/test2.txt"
        self.bucket_name = "testcloudanniekey"
        self.new_bucket_name = "testmovement"
        self.file_name_in_cloud = "created_file1.txt"
        self.new_file_name = "renamed_file.txt"
        self.file_name_after_downloaded = "/home/anniekey/Projects/cloud_pipeline/files/downloaded_file.txt"

        self.AWS_adaptee = adapter_version.AWS_adaptee(
            file_name=self.file_name,
            bucket_name=self.bucket_name,
            new_bucket_name=self.new_bucket_name,
            file_name_in_cloud=self.file_name_in_cloud,
            new_file_name=self.new_file_name,
            file_name_after_downloaded=self.file_name_after_downloaded
        )

    def test_upload_file(self):
        self.AWS_adaptee.upload_file()

        try:
            self.s3.Object(self.bucket_name, self.file_name_in_cloud).load()
            result = True
        except botocore.exceptions.ClientError as error:
            result = False
        self.assertTrue(result)

    def test_download_file(self):
        self.AWS_adaptee.download_file()

        self.assertTrue(os.path.exists(self.file_name_after_downloaded))

    def test_rename_file(self):
        self.AWS_adaptee.rename_file()

        try:
            self.s3.Object(self.bucket_name, self.new_file_name).load()
            result = True
        except botocore.exceptions.ClientError as error:
            result = False
        self.assertTrue(result)

    def test_move_file(self):
        self.AWS_adaptee.move_file()

        try:
            self.s3.Object(self.new_bucket_name, self.new_file_name).load()
            result = True
        except botocore.exceptions.ClientError as error:
            result = False
        self.assertTrue(result)



