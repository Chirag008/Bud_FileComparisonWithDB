import json


class Azure_Creds:
    account_name, account_key, endpoint_suffix, container_name = None, None, None, None

    def __init__(self):
        with open('azure_connection_info.json') as in_fh:
            info_file = json.load(in_fh)
            self.account_name = info_file['azure_connection']['AccountName']
            self.account_key = info_file['azure_connection']['AccountKey']
            self.endpoint_suffix = info_file['azure_connection']['EndpointSuffix']
            self.container_name = info_file['azure_connection']['ContainerName']
