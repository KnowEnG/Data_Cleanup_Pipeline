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
    data_frame_dedup = data_frame.drop_duplicates()

    row_count_diff = len(data_frame.index) - len(data_frame_dedup.index)
    if row_count_diff > 0:
        return data_frame_dedup, "Found duplicate rows and " \
                                       "dropped these duplicates. Proceed to next check."
    if row_count_diff == 0:
        return data_frame_dedup, "No duplication detected in this data set."
    return None,"An unexpected error occured."


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
    data_frame_transpose = data_frame.T
    user_spreadsheet_df_genename_dedup, error_msg = check_duplicate_gene_name(data_frame_transpose)
    if user_spreadsheet_df_genename_dedup is None:
        return False, error_msg
    return user_spreadsheet_df_genename_dedup.T, error_msg


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
    data_frame_genename_dedup = data_frame[~data_frame.index.duplicated()]

    row_count_diff = len(data_frame.index) - len(data_frame_genename_dedup.index)

    if row_count_diff > 0:
        return data_frame_genename_dedup, "Found duplicate gene names " \
                                                "and dropped these duplicates. "

    if row_count_diff == 0:
        return data_frame_genename_dedup,  "No duplication detected in this data set."
    return None, "An unexpected error occurred."


def check_dataframe_value(data_frame, pipeline_type, input_data_type, golden_value_set):
    """
    Checks if the values in user spreadsheet matches with golden standard value set

    Args:
        data_frame: input data frame
        golden_value_set: golden standard value set to be compared with

    Returns:
        match_flag: a flag indicates if the check passes
        error_msg: error message

    """

    if (pipeline_type == 'sc' and input_data_type == 'phenotype'):
        return data_frame, "This user phenotype data passed validation. Proceed to next check."

    if data_frame.isnull().values.any():
        return None, "This user spreadsheet contains invalid NaN value."

    if (pipeline_type == 'sc' or 'gsc' and input_data_type == 'sample'):
        gene_value_set = set(data_frame.ix
                             [:, data_frame.columns != 0].values.ravel())
        if golden_value_set != gene_value_set:
            return None, "This user spreadsheet contains invalid value. " \
                                "Please revise your spreadsheet and reupload."
        else:
            return data_frame, "Value contains in user spreadsheet matches with golden standard value set."

    # check real number negative to positive infinite
    if (pipeline_type == 'gp'):
        if (input_data_type == 'sample'):
            data_frame_filtered = data_frame[data_frame >= 0]
            if data_frame_filtered.empty:
                return None, "Cannot find valid value in user spreadsheet."
            else:
                return data_frame_filtered, True, "Passed value check validtion."
        if (input_data_type == 'phenotype'):
            data_frame_check = data_frame.applymap(lambda x: isinstance(x, (int, float)))
            if (False in data_frame_check.values):
                return None, "Found bad value in user spreadsheet."
            else:
                return data_frame, "Value in user spreadsheet is valid."

    return None, "Value validation goes into a very serious condition. Please double check your input and reupload again."


def check_ensemble_gene_name(data_frame, run_parameters):
    """
    Checks if the gene name follows ensemble format

    Args:
        data_frame: input data frame
        run_parameters: user configuration from run_file

    Returns:
         (match_flag, error_message)

    """
    redis_db = redutil.get_database(run_parameters['redis_credential'])

    data_frame['original'] = data_frame.index

    data_frame.index = data_frame.index.map(
        lambda x: redutil.conv_gene(redis_db, x, run_parameters['source_hint'], run_parameters['taxonid']))

    # filter out the unmapped rows
    mapped_filter = ~data_frame.index.str.contains(r'^unmapped.*$')

    # extracts all the mapped rows in dataframe
    output_df_mapped = data_frame[mapped_filter]

    mapping = data_frame['original']
    mapping_dedup_df = mapping[~mapping.index.duplicated()]

    output_df_mapped = output_df_mapped.drop('original', axis=1)

    output_file_basename = \
        os.path.splitext(os.path.basename(os.path.normpath(run_parameters['spreadsheet_name_full_path'])))[0]

    # writes each data frame to output file separately
    # includes header and index in output file (index is gene name, header is column name)
    output_df_mapped.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_ETL.tsv",
                            sep='\t', header=True, index=True)
    # does not include header in output mapping file
    mapping_dedup_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_MAP.tsv",
                            sep='\t', index=True)

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

    # Case 1: checks if only 0 and 1 appears in user spreadsheet
    user_spreadsheet_val_chked, error_msg = check_dataframe_value(user_spreadsheet_df, 'gp', 'phenotype', default_user_spreadsheet_value)
    if user_spreadsheet_val_chked is None:
        return False, error_msg

    # Case 2: checks the duplication on column name and removes it if exists
    user_spreadsheet_df_col_dedup, error_msg = check_duplicate_column_name(user_spreadsheet_val_chked)
    if user_spreadsheet_df_col_dedup is None:
        return False, error_msg

    # Case 3: checks the duplication on gene name and removes it if exists
    user_spreadsheet_df_genename_dedup, error_msg = check_duplicate_gene_name(user_spreadsheet_df_col_dedup)
    if user_spreadsheet_df_genename_dedup is None:
        return False, error_msg
    # Case 4: checks the validity of gene name
    match_flag, error_msg = check_ensemble_gene_name(user_spreadsheet_df_genename_dedup, run_parameters)
    if match_flag is not None:
        return match_flag, error_msg

    return True, "User spreadsheet has passed the validation successfully! It will be passed to next step..."
