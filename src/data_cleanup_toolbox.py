"""
    This module serves as a connecting function between front end and back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. 
"""
import pandas
import knpackage.redis_utilities as redisutil
import yaml
import os
from enum import Enum


def run_geneset_characterization_pipeline(run_parameters):
    """
    Runs data cleaning for geneset_characterization_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df, phenotype_df = read_input_data_as_df(run_parameters['spreadsheet_name_full_path'],
                                                              run_parameters['phenotype_full_path'])

    # Value check logic a: checks if only 0 and 1 appears in user spreadsheet and rename phenotype data file to have _ETL.tsv suffix
    user_spreadsheet_val_chked, error_msg = check_input_value_for_geneset_characterization(user_spreadsheet_df,
                                                                                           phenotype_df,
                                                                                           run_parameters)

    if user_spreadsheet_val_chked is None:
        return False, error_msg

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    validation_flag, error_msg = sanity_check_data_file(user_spreadsheet_val_chked, run_parameters)

    return validation_flag, error_msg


def run_samples_clustering_pipeline(run_parameters):
    """
    Runs data cleaning for samples_clustering_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df, phenotype_df = read_input_data_as_df(run_parameters['spreadsheet_name_full_path'],
                                                              run_parameters['phenotype_full_path'])

    # Value check logic a: checks if only 0 and 1 appears in user spreadsheet and rename phenotype data file to have _ETL.tsv suffix
    user_spreadsheet_val_chked, error_msg = check_input_value_for_samples_clustering(user_spreadsheet_df,
                                                                                    run_parameters,
                                                                                    phenotype_df)

    if user_spreadsheet_val_chked is None:
        return False, error_msg

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    validation_flag, error_msg = sanity_check_data_file(user_spreadsheet_val_chked, run_parameters)

    return validation_flag, error_msg


def run_gene_priorization_pipeline(run_parameters):
    """
    Runs data cleaning for gene_priorization_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df, phenotype_df = read_input_data_as_df(run_parameters['spreadsheet_name_full_path'],
                                                              run_parameters['phenotype_full_path'])

    # Value check logic b: checks if only 0 and 1 appears in user spreadsheet or if satisfies certain criteria
    user_spreadsheet_val_chked, phenotype_val_checked, error_msg = check_input_value_for_gene_prioritization(
        user_spreadsheet_df, phenotype_df, run_parameters)

    if user_spreadsheet_val_chked is None:
        return False, error_msg

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    validation_flag, error_msg = sanity_check_data_file(user_spreadsheet_val_chked, run_parameters)

    return validation_flag, error_msg


def read_input_data_as_df(spreadsheet_path, phenotype_path):
    """
    Reads two input data: user spreadsheet and phenotype data

    Args:
        spreadsheet_path: full path of user spreadsheet
        phenotype_path: full path of phenotype data

    Returns:
        user_spreadsheet_df: user spreadsheet as data frame
        phenotype_df: phenotype data as data frame
    """
    user_spreadsheet_df = load_data_file(spreadsheet_path)
    phenotype_df = load_data_file(phenotype_path)

    return user_spreadsheet_df, phenotype_df


def get_file_basename(file_path):
    """
    Extracts file basename given the path of a file.

    Args:
        file_path: path of a file.

    Returns:
        output_file_basename: file's basename without suffix.
    """
    output_file_basename = \
        os.path.splitext(os.path.basename(os.path.normpath(file_path)))[0]
    return output_file_basename


def parse_config(run_file_path):
    """
    Parses a configuration file in YAML format

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
    if len(spreadsheet_path) == 0:
        return None

    try:
        user_spreadsheet_df = pandas.read_csv(spreadsheet_path, sep='\t', index_col=0, header=0, mangle_dupe_cols=True)
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
        message: A message indicates the status of current check
    """
    data_frame_dedup = data_frame.drop_duplicates()

    row_count_diff = len(data_frame.index) - len(data_frame_dedup.index)
    if row_count_diff > 0:
        return data_frame_dedup, "Found duplicate rows and " \
                                 "dropped these duplicates. Proceed to next check."
    if row_count_diff == 0:
        return data_frame_dedup, "No duplication detected in this data set."

    return None, "An unexpected error occured during checking duplicate rows."


def check_duplicate_columns(data_frame):
    """
    Checks duplication on entire column and removes it if exists

    Args:
        data_frame: input data frame

    Returns:
        data_frame_dedup_row.T: a data frame in original format
        error_msg: error message
    """
    # transposes original data frame so that original column becomes row
    data_frame_transpose = data_frame.T
    data_frame_dedup_row, error_msg = check_duplicate_rows(data_frame_transpose)

    # transposes back the transposed data frame to be the original format
    return data_frame_dedup_row.T, error_msg


def check_duplicate_column_name(data_frame):
    """
    Checks duplicate column names and rejects it if it exists

    Args: data_frame:
    Returns:
        user_spreadsheet_df_genename_dedup.T: a data frame in original format
        error_msg: error message
    """
    data_frame_transpose = data_frame.T
    user_spreadsheet_df_genename_dedup, error_msg = check_duplicate_row_name(data_frame_transpose)

    if user_spreadsheet_df_genename_dedup is None:
        return False, error_msg

    return user_spreadsheet_df_genename_dedup.T, error_msg


def check_duplicate_row_name(data_frame):
    """
    Checks duplication on gene name and rejects it if it exists

    Args:
        data_frame: input data frame

    Returns:
        data_frame_genename_dedup: a data frame in original format
        error_msg: error message
    """
    data_frame_genename_dedup = data_frame[~data_frame.index.duplicated()]
    row_count_diff = len(data_frame.index) - len(data_frame_genename_dedup.index)

    if row_count_diff > 0:
        return data_frame_genename_dedup, "Found duplicate gene names " \
                                          "and dropped these duplicates. "

    if row_count_diff == 0:
        return data_frame_genename_dedup, "No duplication detected in this data set."

    return None, "An unexpected error occurred during checking duplicate row name."


def check_input_value_for_gene_prioritization(data_frame, phenotype_df, run_parameters):
    """
    This input value check is specifically designed for gene_priorization_pipeline.
    1. user spreadsheet contains real number.
    2. phenotype data contains only positive number, including 0.

    Args:
        data_frame: user spreadsheet as data frame
        phenotype_df: phenotype data as data frame
        run_parameters: configuration as data dictionary

    Returns:
        data_frame_trimed: Either None or trimed data frame will be returned for calling function
        phenotype_trimed: Either None or trimed data frame will be returned for calling function
        message: A message indicates the status of current check
    """
    # drops column which contains NA in data_frame
    data_frame_dropna = data_frame.dropna(axis=1)

    if data_frame_dropna.empty:
        return None, None, "Data frame is empty after remove NA."

    # checks real number negative to positive infinite
    data_frame_check = data_frame_dropna.applymap(lambda x: isinstance(x, (int, float)))

    if False in data_frame_check.values:
        return None, None, "Found not numeric value in user spreadsheet."

    # drops columns with NA value in phenotype dataframe
    phenotype_df = phenotype_df.dropna(axis=1)

    # check phenotype value to be real value bigger than 0
    phenotype_df = phenotype_df[(phenotype_df >= 0).all(1)]

    if phenotype_df.empty:
        return None, None, "Found negative value in phenotype data. Value should be positive."

    phenotype_columns = list(phenotype_df.columns.values)
    data_frame_columns = list(data_frame.columns.values)
    # unordered name
    common_cols = list(set(phenotype_columns) & set(data_frame_columns))

    if not common_cols:
        return None, None, "Cannot find intersection between user spreadsheet column and phenotype data."

    # select common column to process, this operation will reorder the column
    data_frame_trimed = data_frame[common_cols]
    phenotype_trimed = phenotype_df[common_cols]

    if data_frame_trimed.empty:
        return None, None, "Cannot find valid value in user spreadsheet."

    # store cleaned phenotype data to a file
    output_file_basename = get_file_basename(run_parameters['phenotype_full_path'])
    phenotype_trimed.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_ETL.tsv",
                            sep='\t', header=True, index=True)
    return data_frame_trimed, phenotype_trimed, "Passed value check validation."


def check_input_value_for_geneset_characterization(data_frame, phenotype_df, run_parameters):
    """
    Checks if the values in user spreadsheet matches with golden standard value set
        and rename phenotype file to have suffix _ETL.tsv

    Args:
        data_frame: input data frame
        phenotype_df: input phenotype data frame
        golden_value_set: golden standard value set to be compared with

    Returns:
        data_frame: processed data_frame
        message: A message indicates the status of current check
    """
    # defines the default values that can exist in user spreadsheet
    golden_value_set = {0, 1}

    if data_frame.isnull().values.any():
        return None, "This user spreadsheet contains invalid NaN value."

    gene_value_set = set(data_frame.ix[:, data_frame.columns != 0].values.ravel())

    if golden_value_set != gene_value_set:
        return None, "Only 0, 1 are allowed in user spreadsheet. This user spreadsheet contains invalid value: {}. ".format(
            gene_value_set) + \
               "Please revise your spreadsheet and reupload."
    else:
        output_file_basename = get_file_basename(run_parameters['phenotype_full_path'])
        phenotype_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_ETL.tsv",
                            sep='\t', header=True, index=True)
        return data_frame, "Value contains in user spreadsheet matches with golden standard value set."

    return None, "An unexpected condition occurred during value check for either spreadsheet or phenotype data."


def check_input_value_for_samples_clustering(data_frame, run_parameters, phenotype_df=None):
    """
    Checks if the values in user spreadsheet matches with golden standard value set
        and rename phenotype file to have suffix _ETL.tsv

    Args:
        data_frame: input data frame
        phenotype_df: input phenotype data frame
        golden_value_set: golden standard value set to be compared with

    Returns:
        data_frame: processed data_frame
        message: A message indicates the status of current check
    """
    if data_frame.isnull().values.any():
        return None, "This user spreadsheet contains invalid NaN value."

    # checks if it contains only real number
    data_frame_real_number = data_frame.applymap(lambda x: isinstance(x, (int, float)))

    if False in data_frame_real_number.values:
        return None, "Found non-numeric value in user spreadsheet."

    # checks if it contains only positive number
    data_frame_abs = data_frame_real_number.abs()

    if phenotype_df is not None:
        phenotype_df.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
        run_parameters['phenotype_full_path']) + "_ETL.tsv",
                        sep='\t', header=True, index=True)
    data_frame_abs.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
        run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                          sep='\t', header=True, index=True)
    return data_frame_abs, "Value contains in user spreadsheet matches with golden standard value set."


def check_ensemble_gene_name(data_frame, run_parameters):
    """
    Checks if the gene name follows ensemble format.

    Args:
        data_frame: input data frame
        run_parameters: user configuration from run_file

    Returns:
         match_flag: Boolean value indicates the status of current check
         message: A message indicates the current status of current check
    """
    redis_db = redisutil.get_database(run_parameters['redis_credential'])

    data_frame['original'] = data_frame.index

    data_frame.index = data_frame.index.map(
        lambda x: redisutil.conv_gene(redis_db, x, run_parameters['source_hint'], run_parameters['taxonid']))

    # extracts all mapped rows in dataframe
    output_df_mapped = data_frame[~data_frame.index.str.contains(r'^unmapped.*$')]
    output_df_mapped = output_df_mapped.drop('original', axis=1)

    # dedup on gene name mapping dictionary
    mapping = data_frame[['original']]

    mapping_filtered = mapping[~mapping.index.str.contains(r'^unmapped.*$')]

    unmapped_filtered = mapping[mapping.index.str.contains(r'^unmapped.*$')].sort_index(axis=0, ascending=False)
    unmapped_filtered['ensemble'] = unmapped_filtered.index

    mapping_dedup_df = mapping_filtered[~mapping_filtered.index.duplicated()]

    output_file_basename = get_file_basename(run_parameters['spreadsheet_name_full_path'])

    # writes each data frame to output file separately
    # includes header and index in output file (index is gene name, header is column name)
    output_df_mapped.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_ETL.tsv",
                            sep='\t', header=True, index=True)

    # writes unmapped gene name along with return value from Redis data base to a file
    unmapped_filtered.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_UNMAPPED.tsv",
                             sep='\t', header=False, index=False)

    # does not include header in output mapping file
    mapping_dedup_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_MAP.tsv",
                            sep='\t', header=False, index=True)

    if output_df_mapped.empty:
        return False, "No valid ensemble name can be found."
    else:
        return True, "This is a valid user spreadsheet. Proceed to next step analysis."

    return False, "An unexpected error occured during mapping gene name to ensembl name."


def sanity_check_data_file(user_spreadsheet_df, run_parameters):
    """
    Checks the validity of user input spreadsheet data file

    Args:
        user_spreadsheet_df: user spreadsheet input file data frame, which is uploaded from frontend
        run_parameters: run_file parameter dictionary

    Returns:
        flag: Boolean value indicates the status of current check
        message: A message indicates the status of current check
    """
    # Case 1: checks the duplication on column name and removes it if exists
    user_spreadsheet_df_col_dedup, error_msg = check_duplicate_column_name(user_spreadsheet_df)

    if user_spreadsheet_df_col_dedup is None:
        return False, error_msg

    # Case 2: checks the duplication on gene name and removes it if exists
    user_spreadsheet_df_genename_dedup, error_msg = check_duplicate_row_name(user_spreadsheet_df_col_dedup)

    if user_spreadsheet_df_genename_dedup is None:
        return False, error_msg

    # Case 3: checks the validity of gene name meaning if it can be ensemble or not
    match_flag, error_msg = check_ensemble_gene_name(user_spreadsheet_df_genename_dedup, run_parameters)

    if match_flag is not None:
        return match_flag, error_msg

    return True, "User spreadsheet has passed the validation successfully! It will be passed to next step..."

