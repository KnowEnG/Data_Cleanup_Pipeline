#!/usr/bin/env python3

import time
import sys
import os
import input_data_cleaning as dataclng

def main():
    start_time = time.time()

    user_session_path = os.path.dirname(os.getcwd())
    config_file_name = 'run_parameters.yml'

    config_file_path = user_session_path + '/data/run_files/' + config_file_name
    user_config = dataclng.parse_config(config_file_path)
    print(user_config)

    spreadsheet_path = user_config['spreadsheet_name_full_path']
    spreadsheet_df = dataclng.load_data_file(spreadsheet_path)
    is_bad_file, msg = dataclng.sanity_check_data_file(spreadsheet_df, user_config)
    print(msg)

    if is_bad_file is False:
        sys.exit("This is a bad user spreadsheet. Please check syntax before upload.")

    print("--- Program ran for %s seconds ---" % (time.time() - start_time))
    print("Program succeeded!")


if __name__ == "__main__":
    main()
