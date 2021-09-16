import os
from pathlib import Path

from azure.storage.blob import BlobClient, BlobServiceClient


class AzureFileNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)


class Azure_File_Downloader:

    def get_file_from_azure_storage(self, azure_file_extract_name, download_file_path):
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

        # list to capture all the file path having extract name. So that later we can sort it and pick the latest one
        all_extract_name_files = []
        azure_file_extract_name_tokens = azure_file_extract_name.split('/')
        for blob in blobs_list:
            print(blob.name)
            is_name_matched = True
            blob_name_tokens = blob.name.split('/')
            if len(blob_name_tokens) == len(azure_file_extract_name_tokens):
                for file_name_token, blob_name_token in zip(azure_file_extract_name_tokens, blob_name_tokens):
                    if file_name_token == '*':
                        continue
                    elif file_name_token == blob_name_token:
                        continue
                    else:
                        is_name_matched = False
                        break
                if is_name_matched:
                    all_extract_name_files.append(blob.name)
            # if azure_file_extract_name in blob.name:
            #     print("\nDownloading blob to \n\t" + download_file_path)
            #     with open(download_file_path, "wb") as download_file:
            #         print(blob.name + '\n')
            #         blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
            #         download_file.write(blob_client.download_blob().readall())
            #     break
        if len(all_extract_name_files) == 0:
            print(f'No file found matching the azure file pattern -- {azure_file_extract_name}')
            raise AzureFileNotFoundException(f'No file found matching the azure file pattern -- '
                                             f'{azure_file_extract_name}')
        print('\n------------------------------------------------------------------\n')
        print(f'All extract files found --')
        print(*all_extract_name_files, sep='\n')
        print('\n------------------------------------------------------------------')
        # sort the list and pick the latest one
        all_extract_name_files = sorted(all_extract_name_files, reverse=True)
        print(f'sorted all extract files --')
        print(*all_extract_name_files, sep='\n')
        latest_file = all_extract_name_files[0]
        print('-----------------------------------------------------------------')
        print(f'latest file is --  {latest_file}')
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(download_file_path, "wb") as download_file:
            print(latest_file + '\n')
            # blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
            blob_client = container_client.get_blob_client(latest_file)
            download_file.write(blob_client.download_blob().readall())
        print('Downloaded file successfully !!')


if __name__ == '__main__':
    file_downloader = Azure_File_Downloader()
    file_downloader.get_file_from_azure_storage('files/file_azure.txt')
