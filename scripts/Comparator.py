import sys
import threading
import time
import traceback

from reporter.HtmlReporter import HtmlReporter
from data_handler.Azure_File_Downloader import Azure_File_Downloader
from data_handler.Xlsx_File_Handler import Xlsx_File_Handler
from data_handler.Snowflake_DB_Connection_Provider import Snowflake_DB_Connection_Provider
from snowflake.connector import DictCursor
import properties as config
from dateutil.parser import parse


class ErrorEncounterException(Exception):
    def __init__(self, message):
        super().__init__(message)


class ValidationFailedException(Exception):
    def __init__(self, message):
        super().__init__(message)


class Comparator:
    fr = None
    cursor = None
    azure_file_downloader = Azure_File_Downloader()
    columns_to_exclude_in_comparison = ['REPORTDATE']
    out_csv = None
    xlsx_fh = None
    azure_file_name_dict = None
    total_pass = 0
    total_fail = 0

    def __init__(self, file_path, should_download_from_azure, azure_file_extract_name, report_name, table_name,
                 is_header_available, number_of_records_to_match, order_by_columns, sort_file_by_column_numbers,
                 key_column_numbers_in_file, key_column_names_in_db_table):
        self.file_path = file_path
        self.should_download_from_azure = should_download_from_azure
        self.report_name = report_name
        self.table_name = table_name
        self.is_header_available = is_header_available
        self.number_of_records_to_match = number_of_records_to_match
        self.reporter = HtmlReporter(report_name=report_name)
        self.order_by_columns = order_by_columns
        self.sort_file_by_column_numbers = sort_file_by_column_numbers
        self.azure_file_extract_name = azure_file_extract_name
        self.key_column_numbers_in_file = key_column_numbers_in_file
        self.key_column_names_in_db_table = key_column_names_in_db_table

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
        if status == 'fail' and exit_on_failure:
            self.teardown()
            raise ValidationFailedException('Validation failed! Stopped further processing!')
        elif status == 'error' and exit_on_failure:
            self.teardown()
            # raise ErrorEncounterException.ErrorEncounterException('Some error occurred! Stopped processing')

    def start_comparison(self):
        try:
            # get database connection and execute fetch query
            db_con = Snowflake_DB_Connection_Provider().get_db_connection()
            order_by_part_of_query = ''
            for k, v in self.order_by_columns.items():
                order_by_part_of_query += f'{k} {v}, '
            order_by_part_of_query = order_by_part_of_query[:-2]
            sql_query = f'Select * from {self.table_name} order by {order_by_part_of_query}'
            print(f'Querying DB with -- {sql_query}')
            # temporary fix (because file doesn't have column REPORTDATE as of now
            self.order_by_columns.pop('REPORTDATE', None)
            self.cursor = db_con.cursor(DictCursor)
            self.cursor = self.cursor.execute(sql_query)
            result = self.cursor.fetchmany(config.BUFFER_NUMBER_OF_DB_ROWS)

            db_table_headers = [k for k in result[0].keys()]
            # adding db table columns as table headers in html report
            self.reporter.add_db_column_names_as_headers(['FILENAME'] + [k for k in result[0].keys()])
            self.azure_file_name_dict = {'FILENAME': self.azure_file_extract_name}

            # opening an xlsx file to store the comparison result
            self.xlsx_fh = Xlsx_File_Handler(self.report_name.replace('.html', '.xlsx'),
                                             ['FILENAME'] + [k for k in result[0].keys()])

            start = time.time()
            content = self.azure_file_downloader.get_file_content_from_azure_storage(self.azure_file_extract_name)
            end = time.time()
            print(f'Time taken to search and read file content from azure -- {round(end-start)} seconds')
            content = content.decode('utf-8')
            content = content.split('\n')
            if content[-1].strip() == '':
                content = content[:-1]
            # create a hash of content key columns i.e. file key columns
            content_dict = {}
            for line in content:
                tokens = line.split('~')
                key_col_values = []
                for col_number in self.key_column_numbers_in_file:
                    key_col_values.append(tokens[col_number - 1])
                content_dict['-'.join(key_col_values)] = line

            col_count_file = len(content[0].split('~'))
            del content

            number_of_row_checked = 0
            progress_update_count = int(self.number_of_records_to_match / 100)
            slider_count = 1
            print('\nComparison started ... ')
            while True and number_of_row_checked <= self.number_of_records_to_match:
                # iterate all the rows in result set and check against the file
                for db_row in result:
                    if number_of_row_checked == self.number_of_records_to_match:
                        number_of_row_checked += 1
                        break
                    if number_of_row_checked == (progress_update_count * slider_count):
                        self.update_progress(slider_count)
                        slider_count += 1
                    db_row = {k: None if v is None else str(v) for k, v in db_row.items()}
                    # remove the 00:00:00 from date if there is any column with date time as yyyy-mm-dd 00:00:00
                    db_row = {k: v.replace('00:00:00', '').strip() if v is not None else v for k, v in db_row.items()}

                    # check if the key from db row is matched with keys in file content dict
                    # if it's matched then we will proceed with checking other column values otherwise row not found
                    is_order_by_columns_matched = False
                    db_key_col_values = []
                    for col in self.key_column_names_in_db_table:
                        db_key_col_values.append(db_row[col])
                    key_to_match = '-'.join(db_key_col_values)
                    if key_to_match in content_dict.keys():
                        is_order_by_columns_matched = True

                    if not is_order_by_columns_matched:
                        self.total_fail += 1
                        # writing comparison result in xlsx file
                        if self.total_fail <= config.MAX_NUMBER_OF_FAILURE_CASES_TO_REPORT:
                            threading.Thread(target=self.xlsx_fh.write_db_and_file_row_in_xlsx_file,
                                             args=({**self.azure_file_name_dict, **db_row},
                                                   [self.azure_file_name_dict['FILENAME'], 'DB row not found in file !']
                                                   )).start()

                        # writing comparison result in html report
                        threading.Thread(target=self.reporter.add_scenario_result_as_table_formatted_data,
                                         args=('Comparing DB row ',
                                               {**self.azure_file_name_dict, **db_row},
                                               [self.azure_file_name_dict['FILENAME'],
                                                'DB row not found in file !'],
                                               'fail')).start()
                    else:
                        current_row_matched = True
                        unmatched_values = {}
                        row_dict = {}
                        tokens = content_dict[key_to_match].split('~')
                        for token, header in zip(tokens, db_table_headers):
                            row_dict[header] = token

                        for header in db_table_headers:
                            if header in self.columns_to_exclude_in_comparison:
                                continue
                            if row_dict[header] == db_row[header]:
                                continue
                            elif row_dict[header] == '' and db_row[header] is None:
                                continue
                            else:
                                current_row_matched = False
                                unmatched_values[header] = f'{db_row[header]} <==> {row_dict[header]}'

                        if current_row_matched:

                            self.total_pass += 1

                            # writing comparison result in html report
                            threading.Thread(target=self.reporter.add_scenario_result_as_table_formatted_data,
                                             args=('Comparing DB row ',
                                                   {**self.azure_file_name_dict, **db_row},
                                                   {**self.azure_file_name_dict, **row_dict},
                                                   'pass')).start()

                            # writing comparison result in xlsx file
                            if self.total_pass <= config.MAX_NUMBER_OF_SUCCESS_CASES_TO_REPORT:
                                threading.Thread(target=self.xlsx_fh.write_db_and_file_row_in_xlsx_file,
                                                 args=(
                                                     {**self.azure_file_name_dict, **db_row},
                                                     {**self.azure_file_name_dict, **row_dict},
                                                 )).start()
                        else:
                            self.total_fail += 1
                            # writing comparison result in html report
                            threading.Thread(target=self.reporter.add_scenario_result_as_table_formatted_data,
                                             args=('Comparing DB row ',
                                                   {**self.azure_file_name_dict, **db_row},
                                                   {**self.azure_file_name_dict, **row_dict},
                                                   'fail', '',
                                                   [k for k in unmatched_values.keys()])).start()

                            # writing comparison result in xlsx file
                            if self.total_fail <= config.MAX_NUMBER_OF_FAILURE_CASES_TO_REPORT:
                                threading.Thread(target=self.xlsx_fh.write_db_and_file_row_in_xlsx_file,
                                                 args=(
                                                     {**self.azure_file_name_dict, **db_row},
                                                     {**self.azure_file_name_dict, **row_dict},
                                                     [k for k in unmatched_values.keys()]
                                                 )).start()
                    number_of_row_checked += 1

                # fetch next chuck of data from database
                result = self.cursor.fetchmany(config.BUFFER_NUMBER_OF_DB_ROWS)

                # check if we have reached to the end of table
                if not result:
                    break
            self.update_progress(100)
            print('\nprocessed data file completely.')
            self.teardown()
        except Exception as e:
            if isinstance(e, ValidationFailedException):
                return
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
            del self.fr
        if self.cursor is not None:
            print('closing database connection ... !')
            self.cursor.close()
            print('database connection closed successfully !!')
            del self.cursor
        if self.out_csv is not None:
            print('Closing out_csv file ... !')
            self.out_csv.flush()
            self.out_csv.close()
            print('Closed out_csv file successfully !!')
            del self.out_csv
        if self.xlsx_fh is not None:
            self.xlsx_fh.save_xlsx_file()
            del self.xlsx_fh

    def update_progress(self, progress):
        sys.stdout.write('\r[{0}] {1}%'.format('#' * progress, progress))
        sys.stdout.flush()
