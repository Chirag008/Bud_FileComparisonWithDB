import os
from pathlib import Path

from azure.storage.blob import BlobClient, BlobServiceClient


class Azure_File_Downloader:

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
        # blob_client = blob_service_client.get_blob_client(container=container_name, blob=download_file_path)
        container_client = blob_service_client.get_container_client(container_name)
        blobs_list = container_client.list_blobs()

        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        download_file_path = os.path.join(project_root, download_file_path)

        for blob in blobs_list:
            print(blob.name)
            if "cu00000001/202103/20210331/EXTRACT.ACCOUNT" in blob.name:
                print("\nDownloading blob to \n\t" + download_file_path)
                with open(download_file_path, "wb") as download_file:
                    print(blob.name + '\n')
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
                    download_file.write(blob_client.download_blob().readall())
                break


if __name__ == '__main__':
    file_downloader = Azure_File_Downloader()
    file_downloader.get_file_from_azure_storage('files/file_azure.txt')
