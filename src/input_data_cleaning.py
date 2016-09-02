"""
    This module serves as a connecting function between front end with back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. If the user spreadsheet is
     valid or not.
"""
import sys
import pandas
import src.redis_utilities as redutil
import yaml

def load_data_file(spreadsheet_path):
    """
    Loads data file as a data frame object by a given file path
    Args:
        spreadsheet_path: user spreadsheet input file, which is uploaded from frontend

    Returns: user spreadsheet as a data frame

    """
    print("Start checking user spread sheet")
    try:
        user_spreadsheet_df = pandas.read_csv(spreadsheet_path, sep='\t', header=None)
        return user_spreadsheet_df
    except OSError as err:
        sys.exit(str(err))


def check_duplicate_rows(data_frame):
    """
    Checks the duplication on entire row and removes it if it exists

    Args:
        data_frame: input data frame to be checked

    Returns:
          (data_frame_dedup, match_flag, error_message)

    """
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
    gene_value_set = set(data_frame.ix
                         [:, data_frame.columns != 0].values.ravel())
    if golden_value_set != gene_value_set:
        return False, "This user spreadsheet contains invalid value. " \
                      "Please revise your spreadsheet and reupload."
    return True, "Value contains in user spreadsheet matches with golden standard value set."


def check_ensemble_gene_name(data_frame, user_config):
    """
    Checks if the gene name follows ensemble format

    Args:
        data_frame: input data frame
        user_config: user configuration from run_file

    Returns:
         (match_flag, error_message)

    """
    redis_db = redutil.get_database(user_config['redis_credential'])
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
    if not output_df_unmapped_one.empty:
        output_df_unmapped_one.to_csv(user_config['user_spreadsheet_unmapped_gene_output_dir'] + "/tmp_unmapped_one",
                                      header=None, index=None, sep='\t')
        return False, "Found gene names that cannot be mapped to ensemble name."

    if not output_df_unmapped_many.empty:
        output_df_unmapped_many.to_csv(user_config['user_spreadsheet_unmapped_gene_output_dir'] + "/tmp_unmapped_many",
                                       header=None, index=None, sep='\t')
        return False, "Found gene names that mapped to many ensemble name."

    if output_df_mapped.empty:
        return False, "No valid ensemble name can be found."
    else:
        output_df_mapped.to_csv(user_config['user_spreadsheet_mapped_gene_output_dir'] + "/tmp_mapped",
                                header=None, index=None, sep='\t')
        return True, "This is a valid user spreadsheet. Proceed to do next step analysis."

    return False, "An unexpected error occured."


def sanity_check_data_file(user_spreadsheet_df, user_config):
    """
    Checks the validity of user input spreadsheet data file
    Args:
        user_spreadsheet_df: user spreadsheet input file data frame, which is uploaded from frontend
        user_config: run_file parameter dictionary

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
    match_flag, error_msg = check_ensemble_gene_name(user_spreadsheet_df_genename_dedup, user_config)
    if match_flag is not None:
        return match_flag, error_msg

    return True, "This is a valid user spreadsheet! It will be passed to next step..."


def parse_config(config_path):
    """
    Parsing configuration file as yaml format
    Args:
        config_path: run file path

    Returns: a dictionary contains the run file parameters

    """
    with open(config_path, 'r') as stream:
        try:
            config = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config


