import os
from pathlib import Path


class File_Reader:
    file_handler = None
    is_first_row = True
    headers = None

    def __init__(self, file_path, separator='~'):
        self.separator = separator
        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        self.file_path = os.path.join(project_root, file_path)
        if not self.file_handler:
            self.file_handler = open(self.file_path)
        if self.is_first_row:
            self.headers = self.file_handler.readline().rstrip('\n').split(self.separator)
            self.is_first_row = False

    def get_next_row_as_dict(self):
        row = self.file_handler.readline()
        if len(row.rstrip('\n')) == 0:
            return None
        column_data = row.rstrip('\n').split(self.separator)
        return {header: col_data for header, col_data in zip(self.headers, column_data)}

    def close_file(self):
        self.file_handler.close()
