import json

from scripts.Comparator import Comparator


def start_execution():
    with open('files_to_test.json') as in_fh:
        files_to_test = json.load(in_fh)
        for execution_info in files_to_test['files']:
            file_path = execution_info['path']
            report_name = execution_info['report_name']
            table = execution_info['table_name']
            comp = Comparator(file_path=file_path,
                              report_name=report_name,
                              table_name=table)
            comp.start_comparison()


if __name__ == '__main__':
    start_execution()
