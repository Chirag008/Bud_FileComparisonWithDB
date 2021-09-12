import traceback

from reporter.HtmlReporter import HtmlReporter
from data_handler.File_Reader import File_Reader
from data_handler.CSV_File_Handler import CSV_File_Handler
from data_handler.Snowflake_DB_Connection_Provider import Snowflake_DB_Connection_Provider
from snowflake.connector import DictCursor
import properties as config
from dateutil.parser import parse


class Comparator:
    fr = None
    cursor = None

    def __init__(self, file_path, report_name, table_name, is_header_available, number_of_records_to_match,
                 order_by_columns, sort_file_by_column_numbers):
        self.file_path = file_path
        self.report_name = report_name
        self.table_name = table_name
        self.is_header_available = is_header_available
        self.number_of_records_to_match = number_of_records_to_match
        self.reporter = HtmlReporter(report_name=report_name)
        self.order_by_columns = order_by_columns
        self.sort_file_by_column_numbers = sort_file_by_column_numbers

    def validate_result(self,
                        scenario_name,
                        exp_result,
                        actual_result,
                        status=None,
                        exit_on_failure=False,
                        comment=''):

        if status is None:
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
            sql_query = f'Select * from {self.table_name} order by ' \
                        f'{",".join(self.order_by_columns)}'
            self.cursor = db_con.cursor(DictCursor)
            self.cursor = self.cursor.execute(sql_query)
            result = self.cursor.fetchmany(config.BUFFER_NUMBER_OF_DB_ROWS)

            # sort the data file and save it in a temporary location.
            file_handler = CSV_File_Handler()
            temp_file_path = self.file_path.split('.txt')[0] + '_sorted_temp.txt'
            file_handler.sort_and_save_file(self.file_path, temp_file_path,
                                            columns_to_sort_on=self.order_by_columns if self.is_header_available else
                                            self.sort_file_by_column_numbers,
                                            order_ascending=True,
                                            delimiter='~',
                                            is_header_in_file=self.is_header_available)
            # further operations will be handled on sorted file.
            self.file_path = temp_file_path

            # if file header is not available in data file then we will pick the headers from db table
            if self.is_header_available:
                self.fr = File_Reader(self.file_path)
            else:
                self.fr = File_Reader(self.file_path, [k for k in result[0].keys()])
            row_dict = self.fr.get_next_row_as_dict()

            # Check if number of columns in file and database are same in number
            self.validate_result(scenario_name='Validate number of columns same in data file and table',
                                 exp_result=f'Number of columns in DB table - {len(self.fr.headers)}',
                                 actual_result=f'Number of columns in DB table - {len(list(result[0].keys()))}',
                                 exit_on_failure=False)

            # Check if all the columns are same in both data file and DB table (iff header is available in file)
            if self.is_header_available:
                headers_data_file = sorted([h.upper() for h in self.fr.headers])
                headers_db_table = sorted([k.upper() for k in result[0].keys()])
                self.validate_result(scenario_name='Validate column names in data file and db table',
                                     exp_result=headers_data_file,
                                     actual_result=headers_db_table,
                                     exit_on_failure=False)

            number_of_row_checked = 0
            while True and number_of_row_checked <= self.number_of_records_to_match:
                # iterate all the rows in result set and check against the file
                for db_row in result:
                    if number_of_row_checked >= self.number_of_records_to_match:
                        break
                    print(f'Checking for row --->  {db_row}')
                    # check the column by which we sorted the results. If current file row (sorted columns) value
                    # matches then we will check for other columns. otherwise fetch the row until sorted column < db
                    # column
                    is_order_by_columns_matched = True
                    for i, sorted_col in enumerate(self.order_by_columns):
                        while (db_row[sorted_col] != row_dict[sorted_col]) \
                                and row_dict[sorted_col] < db_row[sorted_col]:
                            row_dict = self.fr.get_next_row_as_dict()
                            # now new row should match all the previous ordered columns else we should break from here
                            previous_cols_matched = True
                            for col in range(0, i):
                                if db_row[self.order_by_columns[col]] != row_dict[self.order_by_columns[col]]:
                                    is_order_by_columns_matched = False
                                    previous_cols_matched = False
                                    break
                            if not previous_cols_matched:
                                break

                        if db_row[sorted_col] != row_dict[sorted_col]:
                            is_order_by_columns_matched = False
                            break

                    if not is_order_by_columns_matched:
                        print('Order by columns value not matched in file!')
                        self.validate_result(scenario_name=f'Validating file data Row {number_of_row_checked}',
                                             exp_result=f'database row should be present in file data - {db_row}',
                                             actual_result=f'database row not found in file data')
                    else:
                        current_row_matched = True
                        unmatched_values = {}
                        for header in self.fr.headers:
                            if row_dict[header] == db_row[header]:
                                continue
                            elif row_dict[header] == '' and db_row[header] is None:
                                continue
                            else:
                                current_row_matched = False
                                unmatched_values[header] = f'{db_row[header]} <==> {row_dict[header]}'

                        if current_row_matched:
                            self.validate_result(scenario_name=f'Validating file data Row {number_of_row_checked}',
                                                 exp_result=f'database row should be present in file data - {db_row}',
                                                 actual_result=f'database row present in file data - {row_dict}',
                                                 status='pass')
                        else:
                            self.validate_result(scenario_name=f'Validating file data Row {number_of_row_checked}',
                                                 exp_result=f'database row should be present in file data - {db_row}',
                                                 actual_result=f'database row not matched with file data - {row_dict}',
                                                 status='fail',
                                                 comment=str(unmatched_values))
                        row_dict = self.fr.get_next_row_as_dict()
                    number_of_row_checked += 1

                # fetch next chuck of data from database
                result = self.cursor.fetchmany(config.BUFFER_NUMBER_OF_DB_ROWS)

                # check if we have reached to the end of table
                if not result:
                    break

            # data_row_index = 1
            # while row_dict is not None and data_row_index <= self.number_of_records_to_match:
            #     print(f'comparing row --->  {row_dict}')
            #     is_data_row_found_in_db = False
            #
            #     number_of_rows_iterated_in_db_result = 0
            #     # Search for data row in table result
            #     for db_row in result:
            #         number_of_rows_iterated_in_db_result += 1
            #         current_row_matched = True
            #         for header in self.fr.headers:
            #             if row_dict[header] == db_row[header]:
            #                 continue
            #             elif row_dict[header] == '' and db_row[header] is None:
            #                 continue
            #             else:
            #                 current_row_matched = False
            #                 break
            #         if current_row_matched:
            #             is_data_row_found_in_db = True
            #             break
            #
            #     # if we iterated through multiple result rows before match was found for current row then we need to
            #     # keep the count of rows iterated else make the count as 0
            #     if not is_data_row_found_in_db:
            #         number_of_rows_iterated_in_db_result = 0
            #
            #     self.validate_result(scenario_name=f'Validating file data Row {data_row_index}',
            #                          exp_result='Available in DB',
            #                          actual_result='Available in DB' if is_data_row_found_in_db else f'Not found in '
            #                                                                                          f'DB - {row_dict}')
            #
            #     print(f'Row found in db --  {is_data_row_found_in_db}')
            #     # get next row from file
            #     row_dict = self.fr.get_next_row_as_dict()
            #     if number_of_rows_iterated_in_db_result > 0:
            #         result_next_rows = self.cursor.fetchmany(number_of_rows_iterated_in_db_result)
            #         result_previous_rows = result[number_of_rows_iterated_in_db_result:]
            #         result = result_previous_rows + result_next_rows
            #     data_row_index += 1

            print('processed data file completely.')
            self.teardown()
        except Exception as e:
            print('Some error occurred while processing the file.')
            print(f'full traceback --- {traceback.format_exc()}')
            self.validate_result('Checking for any error while execution',
                                 'No Error should be encountered',
                                 'Error occurred',
                                 exit_on_failure=False,
                                 comment=f'error -- {traceback.format_exc()}')
            self.teardown()

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
        if self.fr is not None:
            print('closing data file ... !')
            self.fr.close_file()
            print('data file closed successfully !!')
        if self.cursor is not None:
            print('closing database connection ... !')
            self.cursor.close()
            print('database connection closed successfully !!')
