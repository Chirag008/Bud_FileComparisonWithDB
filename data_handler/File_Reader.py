import os
from pathlib import Path


class File_Reader:
    file_handler = None
    is_first_row = True
    headers = None

    def __init__(self, file_path, headers=None, separator='~'):
        self.separator = separator
        if not self.file_handler:
            # Get the file handler to the downloaded file
            self.file_handler = self.get_file_handler_from_local(file_path)
        # If headers are not provided to the File Reader then it means header is present in the file itself.
        # So pick the first line as header row, split by the separator and get the headers list
        if not headers:
            if self.is_first_row:
                self.headers = self.file_handler.readline().rstrip('\n').split(self.separator)
                self.is_first_row = False
        else:
            self.headers = headers

    def get_file_handler_from_local(self, file_path):
        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        file_path = os.path.join(project_root, file_path)
        return open(file_path)

    def get_next_row_as_dict(self):
        row = self.file_handler.readline()
        if len(row.rstrip('\n')) == 0:
            return None
        column_data = row.rstrip('\n').split(self.separator)
        return {header: col_data for header, col_data in zip(self.headers, column_data)}

    def get_next_rows_as_list_of_dict(self, number_of_rows):
        rows = []
        for index in range(0, number_of_rows):
            rows.append(self.get_next_row_as_dict())
        return rows

    def close_file(self):
        self.file_handler.close()
