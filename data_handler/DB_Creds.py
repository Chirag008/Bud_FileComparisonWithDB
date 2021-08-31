import json


class DB_Creds:
    username, password, account, warehouse, database, schema = None, None, None, None, None, None

    def __init__(self):
        with open('db_connection_info.json') as in_fh:
            info_file = json.load(in_fh)
            self.username = info_file['db_connection']['username']
            self.password = info_file['db_connection']['password']
            self.account = info_file['db_connection']['account']
            self.warehouse = info_file['db_connection']['warehouse']
            self.database = info_file['db_connection']['database']
            self.schema = info_file['db_connection']['schema']
