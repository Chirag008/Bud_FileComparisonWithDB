import os
from pathlib import Path
from datetime import datetime
import properties as config


class HtmlReporter:
    final_report_text = None
    scenario_number = 0
    data = ''
    text_to_replace_report_name = '<$$REPORT-NAME$$>'
    text_to_replace_execution_data = '<$$DATA$$>'
    text_to_replace_summary_data = '<$$SUMMARY-DATA$$>'
    text_to_replace_execution_time = '<$$REPORT-EXECUTION-TIME$$>'
    text_to_replace_env = '<$$ENV$$>'
    text_to_replace_db_columns = '<$$DB_TABLE_HEADERS$$>'
    total_pass = 0
    total_fail = 0
    total_error = 0
    env = None
    execution_time = None

    def __init__(self, report_name='Automation Report.html', environment='DEV'):
        super()
        project_root = Path(os.path.abspath(os.path.dirname(__file__))).parent
        file_path = os.path.join(project_root, 'template.html')
        with open(file_path, mode='r') as in_fh:
            self.final_report_text = in_fh.read()

        now = datetime.now()
        self.execution_time = now.strftime('%Y-%m-%d %H:%M:%S')
        self.env = environment
        self.report_name = report_name

    def add_db_column_names_as_headers(self, db_table_column_names=None):
        str_db_column_names = ''
        for column in db_table_column_names:
            str_db_column_names += f'<th>{column}</th>'
        self.final_report_text = self.final_report_text.replace(self.text_to_replace_db_columns,
                                                                str_db_column_names)

    def add_scenario_result_as_table_formatted_data(self,
                                                    scenario_name,
                                                    db_data_as_dict,
                                                    row_data_as_list_or_dict,
                                                    status,
                                                    comment='',
                                                    unmatched_columns=None):
        self.scenario_number = self.scenario_number + 1
        if status.upper() == 'PASS':
            class_name = 'pass'
            self.total_pass += 1
            if self.total_pass > config.MAX_NUMBER_OF_SUCCESS_CASES_TO_REPORT:
                return
        elif status.upper() == 'FAIL':
            class_name = 'fail'
            self.total_fail += 1
            if self.total_fail > config.MAX_NUMBER_OF_FAILURE_CASES_TO_REPORT:
                return
        else:
            class_name = 'error'
            self.total_error += 1

        str_db_row = ''
        for val in db_data_as_dict.values():
            str_db_row += f'<td>{"" if val is None else val}</td>\n'
        db_row = f'\n<tr class="{class_name}">\n' \
                 f'<td>{self.scenario_number}</td>\n' \
                 f'<td>{scenario_name}</td>\n' \
                 f'{str_db_row}' \
                 f'<td class="{class_name}">{status}</td>\n' \
                 f'<td style="word-wrap: break-word">{comment}</td>\n' \
                 f'</tr>\n'
        self.data = self.data + db_row

        str_file_row = ''
        if type(row_data_as_list_or_dict) == dict:
            for key, val in row_data_as_list_or_dict.items():
                if unmatched_columns and key in unmatched_columns:
                    str_file_row += f'<td class="mismatch">{val}</td>\n'
                else:
                    str_file_row += f'<td>{val}</td>\n'
        else:
            for val in row_data_as_list_or_dict:
                str_file_row += f'<td>{val}</td>\n'

        file_row = f'\n<tr class="{class_name}">\n' \
                   f'<td></td>\n' \
                   f'<td></td>\n' \
                   f'{str_file_row}' \
                   f'<td class="{class_name}"></td>\n' \
                   f'<td style="word-wrap: break-word"></td>\n' \
                   f'</tr>\n'
        self.data = self.data + file_row

        str_blank_row = ''
        for i in range(0, len(db_data_as_dict)):
            str_blank_row += '<td></td>'
        blank_row = f'\n<tr class="{class_name} blankrow">\n' \
                    f'<td></td>\n' \
                    f'<td></td>\n' \
                    f'{str_blank_row}' \
                    f'<td class="{class_name}"></td>\n' \
                    f'<td style="word-wrap: break-word"></td>\n' \
                    f'</tr>\n'
        self.data = self.data + blank_row


    def add_scenario_result(self,
                            scenario_name,
                            expected_result,
                            actual_result,
                            status,
                            comment=''):

        self.scenario_number = self.scenario_number + 1
        if status.upper() == 'PASS':
            class_name = 'pass'
            self.total_pass += 1
        elif status.upper() == 'FAIL':
            class_name = 'fail'
            self.total_fail += 1
        else:
            class_name = 'error'
            self.total_error += 1

        current_row = f'\n<tr class="{class_name}">\n' \
                      f'<td>{self.scenario_number}</td>\n' \
                      f'<td>{scenario_name}</td>\n' \
                      f'<td>{expected_result}</td>\n' \
                      f'<td>{actual_result}</td>\n' \
                      f'<td class="{class_name}">{status}</td>\n' \
                      f'<td style="word-wrap: break-word">{comment}</td>\n' \
                      f'</tr>\n'
        self.data = self.data + current_row

    def save_report(self, report_file_path='report/customized_html_report'):

        project_root = Path(__file__).parent.parent
        report_file_dir = os.path.join(project_root, report_file_path)
        if not os.path.exists(report_file_dir):
            os.makedirs(report_file_dir)
        report_file_path = os.path.join(report_file_dir, self.report_name)
        self.final_report_text = self.final_report_text.replace(self.text_to_replace_execution_data,
                                                                self.data) \
            .replace(self.text_to_replace_execution_time,
                     self.execution_time) \
            .replace(self.text_to_replace_env, self.env) \
            .replace(self.text_to_replace_report_name, self.report_name.replace('.html', ''))

        summary_table_data = f'\n<tr> \n' \
                             f'<td class="summary-all">{self.scenario_number}</td>\n' \
                             f'<td class="summary-pass">{self.total_pass}</td>\n' \
                             f'<td class="summary-fail">{self.total_fail}</td>\n' \
                             f'<td class="summary-error">{self.total_error}</td>\n' \
                             f'</tr>\n'
        self.final_report_text = self.final_report_text.replace(self.text_to_replace_summary_data,
                                                                summary_table_data)
        with open(report_file_path, mode='w', encoding='utf-8') as out_fh:
            out_fh.write(self.final_report_text)
        print('Saved Report Successfully !!')
