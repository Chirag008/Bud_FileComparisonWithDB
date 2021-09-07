import os
from pathlib import Path

from azure.storage.blob import BlobClient, BlobServiceClient


class File_Reader:
    file_handler = None
    is_first_row = True
    headers = None

    def __init__(self, file_path, headers=None, separator='~'):
        self.separator = separator
        if not self.file_handler:
            # Get the file from azure to local machine
            self.get_file_from_azure_storage(file_path)
            # Get the file handler to the downloaded file
            self.file_handler = self.get_file_handler_from_local(file_path)
        # If headers are not provided to the File Reader then it means header is present in the file itself.
        # So pick the first line as header row, split by the separator and get the headers list
        if not headers:
            if self.is_first_row:
                self.headers = self.file_handler.readline().rstrip('\n').split(self.separator)
                self.is_first_row = False
        else:
            self.headers = headers

    def get_file_from_azure_storage(self, download_file_path):
        connection_string = "DefaultEndpointsProtocol=https;AccountName=sacmfgd02dlxdatapoc2;AccountKey=yjYSUhPn7ucdPoXa43F/bS/wHQqSbvLyZVmqieS/QVEmXRjp7MAjoufq1gqEaSGXDkCK+c4XB2HXTx7X85YYOQ==;EndpointSuffix=core.windows.net"
        try:
            blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="aedigital",
                                                     blob_name="cu00000001/202103/20210331")
            print("connection to azure storage successfully!")
        except:
            print("problem in connecting to azure storage")
        container_name = "aedigital"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=download_file_path)
        blobs_list = blob_client.list_blobs()

        for blob in blobs_list:
            print("\nDownloading blob to \n\t" + download_file_path)
            if "cu00000001/202103/20210331/EXTRACT.ACCOUNT" in blob.name:
                with open(download_file_path, "wb") as download_file:
                    print(blob.name + '\n')
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
                    download_file.write(blob_client.download_blob().readall())

    def get_file_handler_from_local(self, file_path):
        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        file_path = os.path.join(project_root, file_path)
        return open(file_path)

    def get_next_row_as_dict(self):
        row = self.file_handler.readline()
        if len(row.rstrip('\n')) == 0:
            return None
        column_data = row.rstrip('\n').split(self.separator)
        return {header: col_data for header, col_data in zip(self.headers, column_data)}

    def close_file(self):
        self.file_handler.close()
