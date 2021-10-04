import os
from pathlib import Path

from azure.storage.blob import BlobClient, BlobServiceClient
from data_handler.Azure_Creds import Azure_Creds


class AzureFileNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)


class Azure_File_Downloader:
    azure_creds = Azure_Creds()

    def fetch_file_blob_from_azure_storage(self, azure_file_extract_name):
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.azure_creds.account_name};" \
                            f"AccountKey={self.azure_creds.account_key};" \
                            f"EndpointSuffix={self.azure_creds.endpoint_suffix}"
        try:
            blob = BlobClient.from_connection_string(conn_str=connection_string,
                                                     container_name=self.azure_creds.container_name,
                                                     blob_name="cu00000001/202103/20210331")
            print("connected to azure storage successfully!")
        except:
            print("problem in connecting to azure storage")
        container_name = self.azure_creds.container_name
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # blob_client = blob_service_client.get_blob_client(container=container_name, blob=download_file_path)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(azure_file_extract_name)
        return blob_client

        # blobs_list = container_client.list_blobs()

        # # list to capture all the file path having extract name. So that later we can sort it and pick the latest one
        # all_extract_name_files = []
        # azure_file_extract_name_tokens = azure_file_extract_name.split('/')
        # for blob in blobs_list:
        #     print(blob.name)
        #     is_name_matched = True
        #     blob_name_tokens = blob.name.split('/')
        #     if len(blob_name_tokens) == len(azure_file_extract_name_tokens):
        #         for file_name_token, blob_name_token in zip(azure_file_extract_name_tokens, blob_name_tokens):
        #             if file_name_token == '*':
        #                 continue
        #             elif file_name_token == blob_name_token:
        #                 continue
        #             else:
        #                 is_name_matched = False
        #                 break
        #         if is_name_matched:
        #             all_extract_name_files.append(blob.name)
        # if len(all_extract_name_files) == 0:
        #     print(f'No file found matching the azure file pattern -- {azure_file_extract_name}')
        #     raise AzureFileNotFoundException(f'No file found matching the azure file pattern -- '
        #                                      f'{azure_file_extract_name}')
        # print('\n------------------------------------------------------------------\n')
        # print(f'All extract files found --')
        # print(*all_extract_name_files, sep='\n')
        # print('\n------------------------------------------------------------------')
        # # sort the list and pick the latest one
        # all_extract_name_files = sorted(all_extract_name_files, reverse=True)
        # print(f'sorted all extract files --')
        # print(*all_extract_name_files, sep='\n')
        # latest_file = all_extract_name_files[0]
        # print('-----------------------------------------------------------------')
        # print(f'latest file is --  {latest_file}')
        # blob_client = container_client.get_blob_client(latest_file)
        # return blob_client

    def get_file_from_azure_storage(self, azure_file_extract_name, download_file_path):
        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        download_file_path = os.path.join(project_root, download_file_path)
        blob_client = self.fetch_file_blob_from_azure_storage(azure_file_extract_name)
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        print('Downloaded file successfully !!')

    def get_file_content_from_azure_storage(self, azure_file_extract_name):
        blob_client = self.fetch_file_blob_from_azure_storage(azure_file_extract_name)
        return blob_client.download_blob().readall()


if __name__ == '__main__':
    file_downloader = Azure_File_Downloader()
    file_downloader.get_file_content_from_azure_storage('cu00000001/202103/20210331/EXTRACT.ACCOUNT')
