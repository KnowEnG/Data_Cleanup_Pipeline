#!/usr/bin/env python3

import time
import sys
import os
import data_cleanup_toolbox as dataclng
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file

def data_cleanup():
    start_time = time.time()

    run_directory, run_file = get_run_directory_and_file(sys.argv)
    user_config = get_run_parameters(run_directory, run_file)
    '''
    user_session_path = os.path.dirname(os.getcwd())
    config_file_name = 'data_cleanup.yml'
    config_file_path = user_session_path + '/data/run_files/' + config_file_name
    user_config = dataclng.parse_config(config_file_path)
    '''
    spreadsheet_path = user_config['spreadsheet_name_full_path']
    spreadsheet_df = dataclng.load_data_file(spreadsheet_path)
    is_bad_file, msg = dataclng.sanity_check_data_file(spreadsheet_df, user_config)

    if is_bad_file is False:
        sys.exit("This is a bad user spreadsheet. Please check syntax before upload.")

    print("--- Program ran for %s seconds ---" % (time.time() - start_time))
    print("Program succeeded!")


if __name__ == "__main__":
    data_cleanup()
