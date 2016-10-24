#!/usr/bin/env python3

import time
import sys
import data_cleanup_toolbox as dataclng
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file


def data_cleanup():
    start_time = time.time()

    run_directory, run_file = get_run_directory_and_file(sys.argv)
    user_config = get_run_parameters(run_directory, run_file)

    spreadsheet_path = user_config['spreadsheet_name_full_path']
    spreadsheet_df = dataclng.load_data_file(spreadsheet_path)

    phenotype_path = user_config['phenotype_full_path']
    phenotype_df = dataclng.load_data_file(phenotype_path)
    is_bad_file, msg = dataclng.sanity_check_data_file(spreadsheet_df, phenotype_df, user_config)

    if is_bad_file is False:
        sys.exit("This is a bad user spreadsheet. Please check syntax before upload.")

    print("--- Program ran for %s seconds ---" % (time.time() - start_time))
    print("Program succeeded!")


if __name__ == "__main__":
    data_cleanup()
