import traceback

from reporter.HtmlReporter import HtmlReporter
from data_handler.File_Reader import File_Reader
from data_handler.Snowflake_DB_Connection_Provider import Snowflake_DB_Connection_Provider
from snowflake.connector import DictCursor
import properties as config
from dateutil.parser import parse


class Comparator:
    fr = None
    cursor = None

    def __init__(self, file_path, report_name, table_name):
        self.file_path = file_path
        self.report_name = report_name
        self.table_name = table_name
        self.reporter = HtmlReporter(report_name=report_name)

    def validate_result(self,
                        scenario_name,
                        exp_result,
                        actual_result,
                        exit_on_failure=False,
                        comment=''):
        if exp_result == actual_result:
            status = 'pass'
        else:
            status = 'fail'
        if comment != '':
            status = 'error'
        self.reporter.add_scenario_result(scenario_name, str(exp_result), str(actual_result),
                                          status, comment)
        if (status == 'fail' or status == 'error') and exit_on_failure:
            self.teardown()
            exit(1)

    def start_comparison(self):
        try:
            # get database connection and execute fetch query
            db_con = Snowflake_DB_Connection_Provider().get_db_connection()
            sql_query = f'Select * from {self.table_name}'
            self.cursor = db_con.cursor(DictCursor)
            self.cursor = self.cursor.execute(sql_query)
            result = self.cursor.fetchmany(config.BUFFER_NUMBER_OF_DB_ROWS)
            self.fr = File_Reader(self.file_path)
            row_dict = self.fr.get_next_row_as_dict()

            # Check if number of columns in file and database are same in number
            self.validate_result(scenario_name='Validate number of columns same in data file and table',
                                 exp_result=f'Number of columns in DB table - {len(self.fr.headers)}',
                                 actual_result=f'Number of columns in DB table - {len(list(result[0].keys()))}',
                                 exit_on_failure=True)

            # Check if all the columns are same in both data file and DB table
            headers_data_file = sorted([h.upper() for h in self.fr.headers])
            headers_db_table = sorted([k.upper() for k in result[0].keys()])
            self.validate_result(scenario_name='Validate column names in data file and db table',
                                 exp_result=headers_data_file,
                                 actual_result=headers_db_table,
                                 exit_on_failure=True)

            data_file_rows_counter = 1
            data_row_index = 1
            while row_dict is not None:
                print(f'comparing row --->  {row_dict}')
                is_data_row_found_in_db = False
                # Search for data row in table result
                for db_row in result:
                    current_row_matched = True
                    for header in self.fr.headers:
                        if row_dict[header] == db_row[header]:
                            continue
                        elif row_dict[header] == '' and db_row[header] is None:
                            continue
                        else:
                            current_row_matched = False
                            break
                    if current_row_matched:
                        is_data_row_found_in_db = True
                        break

                self.validate_result(scenario_name=f'Validating file data Row {data_row_index}',
                                     exp_result='Available in DB',
                                     actual_result='Available in DB' if is_data_row_found_in_db else f'Not found in '
                                                                                                     f'DB - {row_dict}')

                print(f'Row found in db --  {is_data_row_found_in_db}')
                # get next row from file
                row_dict = self.fr.get_next_row_as_dict()
                data_file_rows_counter += 1
                if data_file_rows_counter >= config.BUFFER_NUMBER_OF_DB_ROWS:
                    result = self.cursor.fetchmany(config.BUFFER_NUMBER_OF_DB_ROWS)
                    data_file_rows_counter = 1
                data_row_index += 1
            print('processed file completely. Closing the file now.')
            self.teardown()
        except Exception as e:
            self.validate_result('Checking for any error while execution',
                                 'No Error should be encountered',
                                 'Error occured',
                                 exit_on_failure=True,
                                 comment=f'error -- {traceback.format_exc()}')


    def is_date(self, string, fuzzy=False):
        """
        Return whether the string can be interpreted as a date.

        :param string: str, string to check for date
        :param fuzzy: bool, ignore unknown tokens in string if True
        """
        try:
            parse(string, fuzzy=fuzzy)
            return True

        except ValueError:
            return False


    def teardown(self):
        self.reporter.save_report()
        self.fr.close_file()
        self.cursor.close()
