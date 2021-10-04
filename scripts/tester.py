import json
import sys
import time

from scripts.Comparator import Comparator


def start_execution():
    count_files_processed = 0
    with open('files_to_test.json') as in_fh:
        files_to_test = json.load(in_fh)
        for execution_info in files_to_test['files']:
            file_path = execution_info['path']
            report_name = execution_info['report_name']
            table = execution_info['table_name']
            order_by_columns = execution_info['order_by_columns']
            sort_file_by_column_numbers = execution_info['sort_file_by_column_numbers']
            key_column_numbers_in_file = execution_info['key_column_number_in_file']
            key_column_names_in_db_table = execution_info['key_column_names_in_db_table']

            # capture the information whether header is available with file or not
            is_header_available = execution_info.get('is_header_available_in_file')
            if is_header_available:
                if is_header_available.lower() in ['yes', 'y', 'true']:
                    is_header_available = True
                else:
                    is_header_available = False
            else:
                is_header_available = False

            # check if number of rows to test is provided. If not provided then default will be all the rows
            number_of_records_to_match = execution_info.get('number_of_records_to_match')
            if number_of_records_to_match is not None:
                try:
                    number_of_records_to_match = int(number_of_records_to_match)
                except ValueError as error:
                    number_of_records_to_match = sys.maxsize
            else:
                number_of_records_to_match = sys.maxsize

            # check if file is to be downloaded from azure storage
            should_download_from_azure = execution_info.get('download_from_azure')
            if should_download_from_azure:
                if should_download_from_azure.lower() in ['yes', 'y', 'true']:
                    should_download_from_azure = True
                else:
                    should_download_from_azure = False
            else:
                should_download_from_azure = False

            # get name of azure file extract
            azure_file_extract_name = None
            if should_download_from_azure:
                azure_file_extract_name = execution_info.get('azure_file_extract_name')
                if azure_file_extract_name is None:
                    print('#########################################################################\n')
                    print(f"*****************  Azure file extract name not provided in json file. "
                          f"So skipped processing for report name - {report_name}. *****************")
                    print('\n#########################################################################')
                    continue
            else:
                azure_file_extract_name = execution_info.get('azure_file_extract_name')
            print(f'========================   Started Comparison for file -- {file_path}  ===========================')
            comp = Comparator(file_path=file_path,
                              should_download_from_azure=should_download_from_azure,
                              azure_file_extract_name=azure_file_extract_name,
                              report_name=report_name,
                              table_name=table,
                              is_header_available=is_header_available,
                              number_of_records_to_match=number_of_records_to_match,
                              order_by_columns=order_by_columns,
                              sort_file_by_column_numbers=sort_file_by_column_numbers,
                              key_column_numbers_in_file=key_column_numbers_in_file,
                              key_column_names_in_db_table=key_column_names_in_db_table)
            start_time = time.time()
            comp.start_comparison()
            # free up memory used by comparator object
            del comp
            end_time = time.time()
            print('=========================  Comparison Done  ========================')
            print(
                f'=======================  Comparison for this file took -- {round(end_time - start_time)} seconds !!')
            count_files_processed += 1
    return count_files_processed


if __name__ == '__main__':
    start = time.time()
    count_files_processed = start_execution()
    print('========================= Exiting the Comparator Program  ========================')
    end = time.time()
    print(f'program finished in --- {round(end - start, 2)} seconds')
    exit(count_files_processed)
