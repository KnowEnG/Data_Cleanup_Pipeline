"""
    This module serves as a connecting function between front end with back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. If the user spreadsheet is
     valid or not.
"""
import pandas
import redis_utilities as redutil
import yaml


def parse_config(run_file_path):
    """
    Parsing a configuration file in YAML format

    Args:
        run_file_path: run file path

    Returns: a dictionary contains the run file parameters

    """
    with open(run_file_path, 'r') as stream:
        try:
            config = yaml.load(stream)
        except yaml.YAMLError as err:
            raise yaml.YAMLError(str(err))
    return config


def load_data_file(spreadsheet_path):
    """
    Loads data file as a data frame object by a given file path

    Args:
        spreadsheet_path: user spreadsheet input file, which is uploaded from frontend

    Returns: user spreadsheet as a data frame

    """
    print("Start checking user spread sheet...")
    try:
        user_spreadsheet_df = pandas.read_csv(spreadsheet_path, sep='\t', header=None)
        return user_spreadsheet_df
    except OSError as err:
        raise OSError(str(err))


def check_duplicate_rows(data_frame):
    """
    Checks the duplication on entire row and removes it if it exists

    Args:
        data_frame: input data frame to be checked

    Returns:
          (data_frame_dedup, match_flag, error_message)

    """
    print("Checking duplicate rows...")
    data_frame_dedup = data_frame.drop_duplicates()

    row_count_diff = len(data_frame.index) - len(data_frame_dedup.index)
    if row_count_diff > 0:
        return data_frame_dedup, True, "Found duplicate rows and " \
                                       "dropped these duplicates. Proceed to next check."
    if row_count_diff == 0:
        return data_frame_dedup, True, "No duplication detected in this data set."
    return data_frame_dedup, False, "An unexpected error occured."


def check_duplicate_gene_name(data_frame):
    """
    Checks the duplication on gene name and rejects it if it exists

    Args:
        data_frame: input data frame

    Returns:
          (data_frame_genename_dedup, match_flag, error_message)

    """
    print("Checking duplicate gene names...")

    data_frame_genename_dedup = data_frame.drop_duplicates(
        0, keep='first').reset_index(drop=True)

    row_count_diff = len(data_frame.index) - len(data_frame_genename_dedup.index)

    if row_count_diff > 0:
        return data_frame_genename_dedup, False, "Found duplicate gene names " \
                                                 "and dropped these duplicates. " \
                                                 "Proceed to next check."
    if row_count_diff == 0:
        return data_frame_genename_dedup, True, "No duplication detected in this data set."
    return data_frame_genename_dedup, False, "An unexpected error occured."


def check_value_set(data_frame, golden_value_set):
    """
    Checks if the values in user spreadsheet matches with golden standard value set

    Args:
        data_frame: input data frame
        golden_value_set: golden standard value set to be compared with

    Returns:
         (match_flag, error_message)

    """
    print("Checking value set...")

    gene_value_set = set(data_frame.ix
                         [:, data_frame.columns != 0].values.ravel())
    if golden_value_set != gene_value_set:
        return False, "This user spreadsheet contains invalid value. " \
                      "Please revise your spreadsheet and reupload."
    return True, "Value contains in user spreadsheet matches with golden standard value set."


def check_ensemble_gene_name(data_frame, run_parameters):
    """
    Checks if the gene name follows ensemble format

    Args:
        data_frame: input data frame
        run_parameters: user configuration from run_file

    Returns:
         (match_flag, error_message)

    """
    print("Checking ensemble gene name ...")

    redis_db = redutil.get_database(run_parameters['redis_credential'])
    num_columns = len(data_frame.columns)

    for idx, row in data_frame.iterrows():
        convert_gene = redutil.conv_gene(redis_db, row[0], '', '9606')
        data_frame.set_value(idx, num_columns, convert_gene)

    # filters the unmappped-none rows
    output_df_unmapped_one = data_frame[
        data_frame[num_columns] == 'unmapped-none'
        ]

    # filters the unmapped-many rows
    output_df_unmapped_many = data_frame[
        data_frame[num_columns] == 'unmapped-many'
        ]

    # filter out the unmapped rows
    mapped_filter = ~data_frame[num_columns].str.contains(r'^unmapped.*$')

    # extracts all the mapped rows in dataframe
    output_df_mapped = data_frame[mapped_filter]

    # writes each data frame to output file separately
    output_df_unmapped_one.to_csv(run_parameters['results_directory'] + "/tmp_unmapped_one",
                                  header=None, index=None, sep='\t')
    output_df_unmapped_many.to_csv(run_parameters['results_directory'] + "/tmp_unmapped_many",
                                   header=None, index=None, sep='\t')
    output_df_mapped.to_csv(run_parameters['results_directory'] + "/tmp_mapped",
                            header=None, index=None, sep='\t')


    if not output_df_unmapped_one.empty or not output_df_unmapped_many.empty:
        return False, "Found gene names that cannot be mapped to ensemble name."

    if output_df_mapped.empty:
        return False, "No valid ensemble name can be found."
    else:
        return True, "This is a valid user spreadsheet. Proceed to next step analysis."

    return False, "An unexpected error occured."


def sanity_check_data_file(user_spreadsheet_df, run_parameters):
    """
    Checks the validity of user input spreadsheet data file

    Args:
        user_spreadsheet_df: user spreadsheet input file data frame, which is uploaded from frontend
        run_parameters: run_file parameter dictionary

    Returns:
        True or False with an error message

    """
    # defines the default values that can exist in user spreadsheet
    default_user_spreadsheet_value = {0, 1}

    # Case 1: checks the duplication on entire row and removes it if it exists
    user_spreadsheet_df_row_dedup, match_flag, error_msg = check_duplicate_rows(user_spreadsheet_df)
    if match_flag is False:
        return match_flag, error_msg

    # Case 2: checks the duplication on gene name and rejects it if it exists
    user_spreadsheet_df_genename_dedup, match_flag, error_msg = check_duplicate_gene_name(user_spreadsheet_df_row_dedup)
    if match_flag is False:
        return match_flag, error_msg

    # Case 3: checks if only 0 and 1 appears in user spreadsheet
    match_flag, error_msg = check_value_set(user_spreadsheet_df_genename_dedup, default_user_spreadsheet_value)
    if match_flag is False:
        return match_flag, error_msg

    # Case 4: checks the validity of gene name
    match_flag, error_msg = check_ensemble_gene_name(user_spreadsheet_df_genename_dedup, run_parameters)
    if match_flag is not None:
        return match_flag, error_msg

    return True, "User spreadsheet has passed the validation successfully! It will be passed to next step..."

