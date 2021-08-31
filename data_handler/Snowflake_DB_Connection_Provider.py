from data_handler.DB_Creds import DB_Creds
import snowflake.connector


class Snowflake_DB_Connection_Provider:
    creds = None

    def __init__(self):
        super()
        self.creds = DB_Creds()

    def get_db_connection(self):
        conn = snowflake.connector.connect(
            user=self.creds.username,
            password=self.creds.password,
            account=self.creds.account,
            warehouse=self.creds.warehouse,
            database=self.creds.database,
            schema=self.creds.schema
        )
        print("**********  Database Connected Successfully  ********** ")
        return conn
