import os
import random
from pathlib import Path


class Test_Data_Generator:
    reference_file = 'files/data_file_from_azure_bud.txt'

    def generate_data_file(self, no_of_rows_required: int, file_path: str):
        out_fh = None
        try:
            project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
            reference_file_path = os.path.join(project_root, self.reference_file)
            lines = []
            with open(reference_file_path) as in_fh:
                line = in_fh.readline()
                while line is not None and line != '':
                    lines.append(line.rstrip('\n'))
                    line = in_fh.readline()
            test_data_file_path = os.path.join(project_root, file_path)
            out_fh = open(test_data_file_path, 'w')
            if no_of_rows_required <= len(lines):
                for index in range(no_of_rows_required):
                    out_fh.write(lines[index] + '\n')
            else:
                for line in lines:
                    out_fh.write(line + '\n')

                no_of_rows_required -= len(lines)
                tokens = lines[len(lines) - 1].split('~')
                account_number = int(tokens[0].replace('CU', ''))
                ordinal = 0
                for index in range(no_of_rows_required):
                    row = random.choice(lines)
                    row_tokens = row.split('~')
                    row_tokens = row_tokens[2:]
                    account_number += 1
                    out_fh.write(f'CU{account_number}~{ordinal}~{"~".join(row_tokens)}\n')


        except Exception as e:
            raise e
        finally:
            if out_fh is not None:
                print('closing output file!')
                out_fh.flush()
                out_fh.close()


if __name__ == '__main__':
    dg = Test_Data_Generator()
    dg.generate_data_file(3000000, 'files/test_data.txt')
