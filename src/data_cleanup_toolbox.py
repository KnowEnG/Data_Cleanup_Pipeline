"""
    This module serves as a connecting function between front end and back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. 
"""
import pandas
import redis_utilities as redutil
import yaml
import os


def parse_config(run_file_path):
    """
    Parsing a configuration file in YAML format

    Args:
        run_file_path: run file path

    Returns:
         config: dictionary contains the run file parameters

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

    Returns:
        user_spreadsheet_df: user spreadsheet as a data frame

    """
    print("Start checking user spread sheet...")
    try:
        user_spreadsheet_df = pandas.read_csv(spreadsheet_path, sep='\t', index_col=0, header=0, mangle_dupe_cols=False)
        return user_spreadsheet_df
    except OSError as err:
        raise OSError(str(err))


def check_duplicate_rows(data_frame):
    """
    Checks duplication on entire row and removes it if exists

    Args:
        data_frame: input data frame to be checked

    Returns:
        data_frame_dedup: a data frame in original formatf
        match_flag: a flag indicates if the check passes
        error_msg: error message

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


def check_duplicate_columns(data_frame):
    """
    Checks duplication on entire column and removes it if exists

    Args:
        data_frame: input data frame

    Returns:
        data_frame_dedup_row.T: a data frame in original format
        match_flag: a flag indicates if the check passes
        error_msg: error message
    """
    print("Checking duplicate columns...")

    # transposes original data frame so that original column becomes row
    data_frame_transpose = data_frame.T
    data_frame_dedup_row, match_flag, error_msg = check_duplicate_rows(data_frame_transpose)
    # transposes back the transposed data frame to be the original format
    return data_frame_dedup_row.T, match_flag, error_msg


def check_duplicate_column_name(data_frame):
    """
    Checks duplicate column names and rejects it if it exists

    Args: data_frame:
    Returns:
        user_spreadsheet_df_genename_dedup.T: a data frame in original format
        match_flag: a flag indicates if the check passes
        error_msg: error message
    """
    print("Checking duplicate column names...")

    data_frame_transpose = data_frame.T
    user_spreadsheet_df_genename_dedup, match_flag, error_msg = check_duplicate_gene_name(data_frame_transpose)
    return user_spreadsheet_df_genename_dedup.T, match_flag, error_msg


def check_duplicate_gene_name(data_frame):
    """
    Checks duplication on gene name and rejects it if it exists

    Args:
        data_frame: input data frame

    Returns:
        data_frame_genename_dedup: a data frame in original format
        match_flag: a flag indicates if the check passes
        error_msg: error message
    """
    print("Checking duplicate gene names...")

    data_frame_genename_dedup = data_frame[~data_frame.index.duplicated()]

    row_count_diff = len(data_frame.index) - len(data_frame_genename_dedup.index)

    if row_count_diff > 0:
        return data_frame_genename_dedup, True, "Found duplicate gene names " \
                                                "and dropped these duplicates. "

    if row_count_diff == 0:
        return data_frame_genename_dedup, True, "No duplication detected in this data set."
    return data_frame_genename_dedup, False, "An unexpected error occurred."


def check_value_set(data_frame, golden_value_set):
    """
    Checks if the values in user spreadsheet matches with golden standard value set

    Args:
        data_frame: input data frame
        golden_value_set: golden standard value set to be compared with

    Returns:
        match_flag: a flag indicates if the check passes
        error_msg: error message

    """
    print("Checking value set...")

    gene_value_set = set(data_frame.ix
                         [:, data_frame.columns != 0].values.ravel())
    if golden_value_set != gene_value_set:
        return False, "This user spreadsheet contains invalid value. " \
                      "Please revise your spreadsheet and reupload."

    data_frame.applymap(lambda x: isinstance(x, (int, float)))
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

    ensemble_gene_name = []
    gene_to_ensemble_map = {}
    for idx, row in data_frame.iterrows():
        convert_gene = redutil.conv_gene(redis_db, idx, '', run_parameters['taxonid'])
        ensemble_gene_name.append(convert_gene)
        print("converted_gene = {}".format(convert_gene))
        gene_to_ensemble_map[idx] = convert_gene

    data_frame.index = ensemble_gene_name
    print(data_frame)
    print(gene_to_ensemble_map)

    # filter out the unmapped rows
    mapped_filter = ~data_frame.index.str.contains(r'^unmapped.*$')

    # extracts all the mapped rows in dataframe
    output_df_mapped = data_frame[mapped_filter]

    output_file_basename = \
    os.path.splitext(os.path.basename(os.path.normpath(run_parameters['spreadsheet_name_full_path'])))[0]

    # writes each data frame to output file separately
    output_df_mapped.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_ETL.tsv",
                            sep='\t')

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

    # Case 1: checks the duplication on column name and removes it if exists
    user_spreadsheet_df_col_dedup, match_flag, error_msg = check_duplicate_column_name(user_spreadsheet_df)
    if match_flag is False:
        return match_flag, error_msg

    # Case 2: checks the duplication on gene name and removes it if exists
    user_spreadsheet_df_genename_dedup, match_flag, error_msg = check_duplicate_gene_name(user_spreadsheet_df_col_dedup)
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
