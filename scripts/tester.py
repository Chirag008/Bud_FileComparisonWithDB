import json
import sys
from scripts.Comparator import Comparator


def start_execution():
    with open('files_to_test.json') as in_fh:
        files_to_test = json.load(in_fh)
        for execution_info in files_to_test['files']:
            file_path = execution_info['path']
            report_name = execution_info['report_name']
            table = execution_info['table_name']

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

            print(f'========================   Started Comparison for file -- {file_path}  ===========================')
            comp = Comparator(file_path=file_path,
                              report_name=report_name,
                              table_name=table,
                              is_header_available=is_header_available,
                              number_of_records_to_match=number_of_records_to_match)
            comp.start_comparison()


if __name__ == '__main__':
    start_execution()
