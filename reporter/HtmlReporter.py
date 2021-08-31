import os
from pathlib import Path
from datetime import datetime


class HtmlReporter:
    final_report_text = None
    scenario_number = 0
    data = ''
    text_to_replace_report_name = '<$$REPORT-NAME$$>'
    text_to_replace_execution_data = '<$$DATA$$>'
    text_to_replace_summary_data = '<$$SUMMARY-DATA$$>'
    text_to_replace_execution_time = '<$$REPORT-EXECUTION-TIME$$>'
    text_to_replace_env = '<$$ENV$$>'
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
                      f'<td>{comment}</td>\n' \
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
                     self.execution_time)\
            .replace(self.text_to_replace_env, self.env)\
            .replace(self.text_to_replace_report_name, self.report_name.replace('.html', ''))

        summary_table_data = f'\n<tr> \n' \
                             f'<td class="summary-all">{self.scenario_number}</td>\n' \
                             f'<td class="summary-pass">{self.total_pass}</td>\n' \
                             f'<td class="summary-fail">{self.total_fail}</td>\n' \
                             f'<td class="summary-error">{self.total_error}</td>\n' \
                             f'</tr>\n'
        self.final_report_text = self.final_report_text.replace(self.text_to_replace_summary_data,
                                                                summary_table_data)
        with open(report_file_path, mode='w') as out_fh:
            out_fh.write(self.final_report_text)
        print('Saved Report Successfully !!')
