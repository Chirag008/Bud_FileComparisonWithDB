import sys
import threading
import time
import traceback

from openpyxl.styles import Font

from reporter.HtmlReporter import HtmlReporter
from data_handler.File_Reader import File_Reader
from data_handler.CSV_File_Handler import CSV_File_Handler
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

            ##########################################################################################
            #       commenting this part because we will no longer download the file
            # download the file to specified path from azure storage
            # if self.should_download_from_azure:
            #     filename = self.azure_file_downloader.get_file_from_azure_storage(
            #         self.azure_file_extract_name,
            #         self.file_path)
            #     self.azure_file_name_dict = {'FILENAME': filename}
            # else:
            #     self.azure_file_name_dict = {'FILENAME': self.azure_file_extract_name}
            ##########################################################################################

            self.azure_file_name_dict = {'FILENAME': self.azure_file_extract_name}

            ##########################################################################################
            # commenting sorting of file because it seems not necessary as of now
            # sort the data file and save it in a temporary location.
            # file_handler = CSV_File_Handler()
            # temp_file_path = self.file_path.split('.txt')[0] + '_sorted_temp.txt'
            # file_handler.sort_and_save_file(self.file_path, temp_file_path,
            #                                 columns_to_sort_on=self.order_by_columns if self.is_header_available else
            #                                 self.sort_file_by_column_numbers,
            #                                 order_ascending=True,
            #                                 delimiter='~',
            #                                 is_header_in_file=self.is_header_available)
            ##########################################################################################

            ########################################################################################
            # (No more required. Now storing in xlsx file. So commenting it)
            # open a csv file for writing output
            # self.out_csv = file_handler.get_new_csv_file_to_write(self.report_name.replace('.html', '.csv'))
            # write headers from db table into csv file
            # db_headers = [k.upper() for k in result[0].keys()]
            # self.out_csv.write(','.join(db_headers) + '\n')
            ########################################################################################

            # releasing memory taken up by file_handler
            # del file_handler

            # opening an xlsx file to store the comparison result
            self.xlsx_fh = Xlsx_File_Handler(self.report_name.replace('.html', '.xlsx'),
                                             ['FILENAME'] + [k for k in result[0].keys()])

            # further operations will be handled on sorted file.
            # self.file_path = temp_file_path

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
                    key_col_values.append(tokens[col_number-1])
                content_dict['-'.join(key_col_values)] = line

            # if file header is not available in data file then we will pick the headers from db table
            # if self.is_header_available:
            #     self.fr = File_Reader(self.file_path)
            # else:
            #     self.fr = File_Reader(self.file_path, [k for k in result[0].keys()])
            # row_dict = self.fr.get_next_row_as_dict()

            # Check if number of columns in file and database are same in number
            db_col_count = len(list(result[0].keys()))
            # self.validate_result(scenario_name='Validate number of columns same in data file and table',
            #                      exp_result=f'columns in DB table - [{db_col_count}] and data file - [{db_col_count}]',
            #                      actual_result=f'columns in DB table - [{db_col_count}] and data file - '
            #                                    f'[{self.fr.num_of_cols}]',
            #                      exit_on_failure=False)

            # Check if all the columns are same in both data file and DB table (iff header is available in file)
            if self.is_header_available:
                headers_data_file = sorted([h.upper() for h in self.fr.headers])
                headers_db_table = sorted([k.upper() for k in result[0].keys()])
                # self.validate_result(scenario_name='Validate column names in data file and db table',
                #                      exp_result=headers_data_file,
                #                      actual_result=headers_db_table,
                #                      exit_on_failure=True)
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

                    # print(f'Checking for row --->  {db_row}')
                    # check the column by which we sorted the results. If current file row (sorted columns) value
                    # matches then we will check for other columns. otherwise fetch the row until file column < db
                    # column
                    is_order_by_columns_matched = False
                    # order_by_columns_list = list(self.order_by_columns.keys())
                    # for i, sorted_col in enumerate(order_by_columns_list):
                    #     while (db_row[sorted_col] != row_dict[sorted_col]) \
                    #             and row_dict[sorted_col] < db_row[sorted_col]:
                    #         row_dict = self.fr.get_next_row_as_dict()
                    #         # now new row should match all the previous ordered columns else we should break from here
                    #         previous_cols_matched = True
                    #         for col in range(0, i):
                    #             if db_row[order_by_columns_list[col]] != row_dict[order_by_columns_list[col]]:
                    #                 is_order_by_columns_matched = False
                    #                 previous_cols_matched = False
                    #                 break
                    #         if not previous_cols_matched:
                    #             break
                    #
                    #     if db_row[sorted_col] != row_dict[sorted_col]:
                    #         is_order_by_columns_matched = False
                    #         break

                    # key_to_match = db_row['PARENTACCOUNT'] + '-' + db_row['ORDINAL']
                    db_key_col_values = []
                    for col in self.key_column_names_in_db_table:
                        db_key_col_values.append(db_row[col])
                    key_to_match = '-'.join(db_key_col_values)
                    if key_to_match in content_dict.keys():
                        is_order_by_columns_matched = True

                    if not is_order_by_columns_matched:
                        self.total_fail += 1
                        # print('Order by columns value not matched in file!')
                        # self.validate_result(scenario_name=f'Validating file data Row {number_of_row_checked}',
                        #                      exp_result=f'database row should be present in file data - {db_row}',
                        #                      actual_result=f'database row not found in file data')
                        ########################################################################################
                        # (No more required. Now storing in xlsx file. So commenting it)
                        # writing db unmatched row in csv file
                        # db_row_values = [str(v) if v is not None else '' for v in db_row.values()]
                        # self.out_csv.write(','.join(db_row_values) + '\n')
                        ########################################################################################
                        # writing in xlsx file that -- data not found for this row in file

                        if self.total_fail <= config.MAX_NUMBER_OF_FAILURE_CASES_TO_REPORT:
                            threading.Thread(target=self.xlsx_fh.write_db_and_file_row_in_xlsx_file,
                                             args=({**self.azure_file_name_dict, **db_row},
                                                   [self.azure_file_name_dict['FILENAME'], 'DB row not found in file !']
                                                   )).start()
                            # writing db row in xlsx file
                            # self.xlsx_fh.write_dict_as_row_in_xlsx_file({**self.azure_file_name_dict, **db_row})
                            # threading.Thread(target=self.xlsx_fh.write_row_in_xlsx_file,
                            #                  args=([self.azure_file_name_dict['FILENAME'],
                            #                         'DB row not found in file !'], Font(color='FF0000'))).start()
                            # self.xlsx_fh.write_row_in_xlsx_file([self.azure_file_name_dict['FILENAME'],
                            #                                      'DB row not found in file !'], Font(color='FF0000'))
                            # threading.Thread(target=self.xlsx_fh.write_blank_colored_row_in_xlsx_file, args=()).start()

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

                        # content_dict.pop(key_to_match)

                        if current_row_matched:
                            # self.validate_result(scenario_name=f'Validating file data Row {number_of_row_checked}',
                            #                      exp_result=f'database row should be present in file data - {db_row}',
                            #                      actual_result=f'database row present in file data - {row_dict}',
                            #                      status='pass')
                            self.total_pass += 1

                            threading.Thread(target=self.reporter.add_scenario_result_as_table_formatted_data,
                                             args=('Comparing DB row ',
                                                   {**self.azure_file_name_dict, **db_row},
                                                   {**self.azure_file_name_dict, **row_dict},
                                                   'pass')).start()
                            if self.total_pass <= config.MAX_NUMBER_OF_SUCCESS_CASES_TO_REPORT:
                                threading.Thread(target=self.xlsx_fh.write_db_and_file_row_in_xlsx_file,
                                                 args=(
                                                     {**self.azure_file_name_dict, **db_row},
                                                     {**self.azure_file_name_dict, **row_dict},
                                                 )).start()
                                # writing the file row in xlsx file
                                # threading.Thread(target=self.xlsx_fh.write_dict_as_row_in_xlsx_file,
                                #                  args=({**self.azure_file_name_dict, **row_dict}, [])).start()
                                # threading.Thread(target=self.xlsx_fh.write_blank_colored_row_in_xlsx_file, args=())
                        else:
                            # self.validate_result(scenario_name=f'Validating file data Row {number_of_row_checked}',
                            #                      exp_result=f'database row should be present in file data - {db_row}',
                            #                      actual_result=f'database row not matched with file data - {row_dict}',
                            #                      status='fail',
                            #                      comment=str(unmatched_values))
                            self.total_fail += 1
                            threading.Thread(target=self.reporter.add_scenario_result_as_table_formatted_data,
                                             args=('Comparing DB row ',
                                                   {**self.azure_file_name_dict, **db_row},
                                                   {**self.azure_file_name_dict, **row_dict},
                                                   'fail', '',
                                                   [k for k in unmatched_values.keys()])).start()

                            if self.total_fail <= config.MAX_NUMBER_OF_FAILURE_CASES_TO_REPORT:
                                threading.Thread(target=self.xlsx_fh.write_db_and_file_row_in_xlsx_file,
                                                 args=(
                                                     {**self.azure_file_name_dict, **db_row},
                                                     {**self.azure_file_name_dict, **row_dict},
                                                     [k for k in unmatched_values.keys()]
                                                 )).start()
                                # writing the file row in xlsx file
                                # threading.Thread(target=self.xlsx_fh.write_dict_as_row_in_xlsx_file,
                                #                  args=({**self.azure_file_name_dict, **row_dict},
                                #                        [k for k in unmatched_values.keys()])).start()
                                # threading.Thread(target=self.xlsx_fh.write_blank_colored_row_in_xlsx_file, args=()).start()
                        # row_dict = self.fr.get_next_row_as_dict()
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
