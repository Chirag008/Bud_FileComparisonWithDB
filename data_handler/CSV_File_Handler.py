import os

import pandas as pd
from pathlib import Path


class CSV_File_Handler:
    def get_new_csv_file_to_write(self, file_name):
        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        file_dir = 'report/csv_output'
        file_dir = os.path.join(project_root, file_dir)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        file_path = os.path.join(file_dir, file_name)
        return open(file_path, 'w')

    def sort_and_save_file(self, file_to_sort, sorted_file_path, columns_to_sort_on: dict, order_ascending=True,
                           delimiter=',', is_header_in_file=True):
        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        file_to_sort = os.path.join(project_root, file_to_sort)
        sorted_file_path = os.path.join(project_root, sorted_file_path)
        if is_header_in_file:
            csv_data = pd.read_csv(file_to_sort, sep=delimiter, index_col=False)
            columns = [col_name for col_name in columns_to_sort_on.keys()]
        else:
            csv_data = pd.read_csv(file_to_sort, sep=delimiter, index_col=False, header=None)
            columns = [(int(col_index) - 1) for col_index in columns_to_sort_on.keys()]
        print("\nBefore sorting:\nFirst 10 rows of file -- ")
        print(csv_data.head(10))

        # order_ascending = [order_ascending for i in range(0, len(columns_to_sort_on))]
        order = [True if v.lower() == 'asc' else False for v in columns_to_sort_on.values()]
        # sort data frame
        csv_data.sort_values(columns,
                             axis=0,
                             ascending=order,
                             inplace=True)

        # displaying sorted data frame
        print("\nAfter sorting:\nFirst 10 rows of file -- ")
        print(csv_data.head(10))

        print(f'Saving the sorted file at location - {sorted_file_path}')
        if is_header_in_file:
            csv_data.to_csv(path_or_buf=sorted_file_path, sep=delimiter, index=False)
        else:
            csv_data.to_csv(path_or_buf=sorted_file_path, sep=delimiter, index=False, header=None)
        print('Sorted file saved successfully!')


if __name__ == '__main__':
    cfh = CSV_File_Handler()
    cfh.sort_and_save_file('files/data_2.txt', 'files/data_2_sorted_temp.txt',
                           columns_to_sort_on={'1': "DESC", '2': "ASC"},
                           order_ascending=True,
                           delimiter='~',
                           is_header_in_file=False)
