"""
    This module serves as a connecting function between front end and back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. 
"""
import pandas
import knpackage.redis_utilities as redisutil
import os

logging = []

def run_geneset_characterization_pipeline(run_parameters):
    """
    Runs data cleaning for geneset_characterization_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])

    if user_spreadsheet_df.empty:
        logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
            run_parameters['spreadsheet_name_full_path']))
        return False, logging

    # Value check logic a: checks if only 0 and 1 appears in user spreadsheet and rename phenotype data file to have _ETL.tsv suffix
    user_spreadsheet_val_chked = check_input_value_for_geneset_characterization(user_spreadsheet_df)

    if user_spreadsheet_val_chked is None:
        return False, logging

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if user_spreadsheet_df_cleaned is None:
        return False, logging

    user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
        run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                       sep='\t', header=True, index=True)
    logging.append(
        "INFO: Cleaned user spreadsheet has {} rows, {} columns.".format(user_spreadsheet_df_cleaned.shape[0],
                                                                         user_spreadsheet_df_cleaned.shape[1]))
    return True, logging


def run_samples_clustering_pipeline(run_parameters):
    """
    Runs data cleaning for samples_clustering_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])
    if user_spreadsheet_df is None or user_spreadsheet_df.empty:
        logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
            run_parameters['spreadsheet_name_full_path']))
        return False, logging

    phenotype_df_cleaned = None
    if 'phenotype_name_full_path' in run_parameters.keys():
        logging.append("INFO: Start processing phenotype data.")
        phenotype_df = load_data_file(run_parameters['phenotype_name_full_path'])
        if phenotype_df is None or phenotype_df.empty:
            logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
                run_parameters['phenotype_name_full_path']))
            return False, logging
        else:
            phenotype_df_cleaned = run_pre_processing_phenotype_data(phenotype_df, user_spreadsheet_df.columns.values)
            if phenotype_df_cleaned is None:
                return False, logging

    logging.append("INFO: Start processing user spreadsheet data.")
    # Value check logic a: checks if only real number appears in user spreadsheet and create absolute value
    user_spreadsheet_val_chked = check_input_value_for_samples_clustering(user_spreadsheet_df)

    if user_spreadsheet_val_chked is None:
        return False, logging

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if user_spreadsheet_df_cleaned is None:
        return False, logging
    else:
        user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                           sep='\t', header=True, index=True)
        logging.append(
            "INFO: Cleaned user spreadsheet has {} rows, {} columns.".format(user_spreadsheet_df_cleaned.shape[0],
                                                                             user_spreadsheet_df_cleaned.shape[1]))
    if phenotype_df_cleaned is not None:
        phenotype_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['phenotype_name_full_path']) + "_ETL.tsv",
                                    sep='\t', header=True, index=True)
        logging.append(
            "INFO: Cleaned phenotype data has {} rows, {} columns.".format(phenotype_df_cleaned.shape[0],
                                                                           phenotype_df_cleaned.shape[1]))
    return True, logging


def run_gene_prioritization_pipeline(run_parameters):
    """
    Runs data cleaning for gene_prioritization_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])

    if user_spreadsheet_df is None or user_spreadsheet_df.empty:
        logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
            run_parameters['spreadsheet_name_full_path']))
        return False, logging

    phenotype_df = load_data_file(run_parameters['phenotype_name_full_path'])

    if phenotype_df is None or phenotype_df.empty:
        logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
            run_parameters['phenotype_name_full_path']))
        return False, logging

    # Value check logic b: checks if only 0 and 1 appears in user spreadsheet or if satisfies certain criteria
    user_spreadsheet_val_chked, phenotype_val_checked = check_input_value_for_gene_prioritization(
        user_spreadsheet_df, phenotype_df, run_parameters['correlation_measure'])

    if user_spreadsheet_val_chked is None:
        return False, logging

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if user_spreadsheet_df_cleaned is None or phenotype_val_checked is None:
        return False, logging

    # store cleaned phenotype data to a file
    phenotype_val_checked.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
        run_parameters['phenotype_name_full_path']) + "_ETL.tsv",
                                 sep='\t', header=True, index=True)
    user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
        run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                       sep='\t', header=True, index=True)
    logging.append(
        "INFO: Cleaned user spreadsheet has {} rows, {} columns.".format(user_spreadsheet_df_cleaned.shape[0],
                                                                         user_spreadsheet_df_cleaned.shape[1]))
    return True, logging


def remove_na_index(dataframe):
    """
    Remove rows contains NA as index.
    Args:
        dataframe: input dataframe to be cleaned

    Returns:
        dataframe_rm_na_idx: a cleaned dataframe
    """
    org_row_cnt = dataframe.shape[0]
    dataframe_rm_na_idx = dataframe[pandas.notnull(dataframe.index)]
    new_row_cnt = dataframe_rm_na_idx.shape[0]
    diff = org_row_cnt - new_row_cnt
    if diff > 0:
        logging.append("WARNING: Removed {} row(s) which contains NA in index.".format(diff))
    if dataframe_rm_na_idx.empty:
        logging.append(
            "ERROR: After removed {} row(s) that contains NA in index, the dataframe becames empty.".format(diff))
        return None
    logging.append("INFO: No NA detected in row index.")
    return dataframe_rm_na_idx


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


def load_data_file(file_path):
    """
    Loads data file as a data frame object by a given file path

    Args:
        spreadsheet_path: user spreadsheet input file, which is uploaded from frontend

    Returns:
        user_spreadsheet_df: user spreadsheet as a data frame
    """
    if file_path == None:
        logging.append("ERROR: Input file path is empty. Please provide a valid input path.")
        return None

    try:
        input_df = pandas.read_csv(file_path, sep='\t', index_col=0, header=0, mangle_dupe_cols=False)
        logging.append("INFO: Successfully loaded input data: {}.".format(file_path))
        return input_df
    except OSError as err:
        logging.append("ERROR: {}".format(str(err)))
        return None


def validate_load_data_file(file_path):
    ret_df = load_data_file(file_path)
    if ret_df.empty:
        ret_msg = "Input data in {} is empty. Please provide a valid input data.".format(file_path)
        logging.append(ret_msg)
        return False


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
        return data_frame_dedup, "WARNING: Found duplicate rows and " \
                                 "dropped these duplicates. Proceed to next check."
    if row_count_diff == 0:
        return data_frame_dedup, "No row duplication detected in this data set."

    return None, "ERROR: An unexpected error occured during checking duplicate rows."


def check_duplicate_columns(data_frame):
    """
    Checks duplication on entire column and removes it if exists

    Args:
        data_frame: input data frame

    Returns:
        data_frame_dedup_row.T: a data frame in original format
        ret_msg: error message
    """
    # transposes original data frame so that original column becomes row
    data_frame_transpose = data_frame.T
    data_frame_dedup_row, ret_msg = check_duplicate_rows(data_frame_transpose)

    # transposes back the transposed data frame to be the original format
    return data_frame_dedup_row.T, ret_msg


def check_duplicate_column_name(data_frame):
    """
    Checks duplicate column names and rejects it if it exists

    Args: data_frame:
    Returns:
        user_spreadsheet_df_genename_dedup.T: a data frame in original format
        ret_msg: error message
    """
    data_frame_transpose = data_frame.T
    data_frame_row_dedup = data_frame_transpose[~data_frame_transpose.index.duplicated()]
    if data_frame_row_dedup.empty:
        logging.append("ERROR: User spreadsheet becomes empty after remove column duplicates.")
        return False

    row_count_diff = len(data_frame_transpose.index) - len(data_frame_row_dedup.index)

    if row_count_diff > 0:
        logging.append("WARNING: Removed {} duplicate column(s) from user spreadsheet.".format(row_count_diff))
        return data_frame_row_dedup.T

    if row_count_diff == 0:
        logging.append("INFO: No duplicate column name detected in this data set.")
        return data_frame_row_dedup.T

    if row_count_diff < 0:
        logging.append("ERROR: An unexpected error occurred during checking duplicate column name.")
        return None


def check_duplicate_row_name(data_frame):
    """
    Checks duplication on gene name and rejects it if it exists


    Args:
        data_frame: input data frame

    Returns:
        data_frame_genename_dedup: a data frame in original format
        ret_msg: error message
    """
    data_frame_genename_dedup = data_frame[~data_frame.index.duplicated()]
    if data_frame_genename_dedup.empty:
        logging.append("ERROR: User spreadsheet becomes empty after remove column duplicates.")
        return False

    row_count_diff = len(data_frame.index) - len(data_frame_genename_dedup.index)
    if row_count_diff > 0:
        logging.append("WARNING: Removed {} duplicate row(s) from user spreadsheet.".format(row_count_diff))
        return data_frame_genename_dedup

    if row_count_diff == 0:
        logging.append("INFO: No duplicate row name detected in this data set.")
        return data_frame_genename_dedup

    if row_count_diff < 0:
        logging.append("ERROR: An unexpected error occurred during checking duplicate row name.")
        return None


def check_input_value_for_gene_prioritization(data_frame, phenotype_df, correlation_measure):
    # drops column which contains NA in data_frame
    data_frame_dropna = data_frame.dropna(axis=1)

    if data_frame_dropna.empty:
        logging.append("User spreadsheet is empty after removing NA.")
        return None, None

    # checks real number negative to positive infinite
    data_frame_check = data_frame_dropna.applymap(lambda x: isinstance(x, (int, float)))

    if False in data_frame_check.values:
        logging.append("ERROR: Found non-numeric value in user spreadsheet.")
        return None, None

    # defines the default values that can exist in phenotype data
    gold_value_set = {0, 1}

    if correlation_measure == 't_test':
        phenotype_value_set = set(phenotype_df.ix[:, phenotype_df.columns != 0].values.ravel())
        if gold_value_set != phenotype_value_set:
            logging.append(
                "ERROR: Only 0, 1 are allowed in phenotype data when running t_test. This phenotype data contains invalid value: {}. ".format(
                    phenotype_value_set) + "Please revise your phenotype and reupload.")
            return None, None

    if correlation_measure == 'pearson':
        phenotype_df_check = phenotype_df.applymap(lambda x: isinstance(x, (int, float)))
        if False in phenotype_df_check:
            logging.append("ERROR: Only numeric value is allowed in phenotype data when running pearson test. Found non-numeric value in phenotype data.")
            return None, None
    return data_frame_dropna, phenotype_df


def check_input_value_for_geneset_characterization(data_frame):
    """
    Checks if the values in user spreadsheet passed data validation
        and rename phenotype file to have suffix _ETL.tsv

    Args:
        data_frame: input data frame
        phenotype_df: input phenotype data frame
        gold_value_set: gold standard value set to be compared with

    Returns:
        data_frame: processed data_frame
        message: A message indicates the status of current check
    """
    # defines the default values that can exist in user spreadsheet
    gold_value_set = {0, 1}

    if data_frame.isnull().values.any():
        logging.append("ERROR: This user spreadsheet contains invalid NaN value.")
        return None

    gene_value_set = set(data_frame.ix[:, data_frame.columns != 0].values.ravel())

    if gold_value_set != gene_value_set:
        logging.append(
            "ERROR: Only 0, 1 are allowed in user spreadsheet. This user spreadsheet contains invalid value: {}. ".format(
                gene_value_set) + "Please revise your spreadsheet and reupload.")
        return None

    return data_frame


def check_input_value_for_samples_clustering(data_frame):
    """
    Checks if the values in user spreadsheet passed data validation
        and rename phenotype file to have suffix _ETL.tsv

    Args:
        data_frame: input data frame
        phenotype_df: input phenotype data frame
        gold_value_set: gold standard value set to be compared with

    Returns:
        data_frame: processed data_frame
        message: A message indicates the status of current check
    """
    if data_frame.isnull().values.any():
        logging.append("ERROR: This user spreadsheet contains invalid NaN value.")
        return None

    # checks if it contains only real number
    data_frame_real_number = data_frame.applymap(lambda x: isinstance(x, (int, float)))

    if False in data_frame_real_number.values:
        logging.append("ERROR: Found non-numeric value in user spreadsheet.")
        return None

    # checks number of negative values
    data_frame_negative_cnt = data_frame.lt(0).sum().sum()
    if data_frame_negative_cnt > 0:
        logging.append("WARNING: Converted {} negative number to their positive value.".format(data_frame_negative_cnt))

    # checks if it contains only positive number
    data_frame_abs = data_frame.abs()

    return data_frame_abs


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

    # disable this flag to avoid SettingWithCopyWarning
    data_frame.is_copy = False
    data_frame['original'] = data_frame.index

    data_frame.index = data_frame.index.map(
        lambda x: redisutil.conv_gene(redis_db, x, run_parameters['source_hint'], run_parameters['taxonid']))

    # extracts all mapped rows in dataframe
    output_df_mapped = data_frame[~data_frame.index.str.contains(r'^unmapped.*$')]
    output_df_mapped = output_df_mapped.drop('original', axis=1)

    # dedup on gene name mapping dictionary
    mapping = data_frame[['original']]

    mapping_filtered = mapping[~mapping.index.str.contains(r'^unmapped.*$')]

    logging.append("INFO: Mapped {} genes to ensemble name.".format(mapping_filtered.shape[0]))

    unmapped_filtered = mapping[mapping.index.str.contains(r'^unmapped.*$')].sort_index(axis=0, ascending=False)
    unmapped_filtered['ensemble'] = unmapped_filtered.index

    if unmapped_filtered.shape[0] > 0:
        logging.append("INFO: Unable to map {} genes to ensemble name.".format(unmapped_filtered.shape[0]))

    mapping_dedup_df = mapping_filtered[~mapping_filtered.index.duplicated()]

    output_file_basename = get_file_basename(run_parameters['spreadsheet_name_full_path'])

    # writes unmapped gene name along with return value from Redis database to a file
    unmapped_filtered.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_UNMAPPED.tsv",
                             sep='\t', header=False, index=False)

    # writes dedupped mapping between original gene name and ensemble name to a file
    mapping_dedup_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_MAP.tsv",
                            sep='\t', header=False, index=True)

    if output_df_mapped.empty:
        logging.append("ERROR: No valid ensemble name can be found.")
        return None

    return output_df_mapped


def sanity_check_user_spreadsheet(user_spreadsheet_df, run_parameters):
    """
    Checks the validity of user input spreadsheet data file

    Args:
        user_spreadsheet_df: user spreadsheet input file data frame, which is uploaded from frontend
        run_parameters: run_file parameter dictionary

    Returns:
        flag: Boolean value indicates the status of current check
        message: A message indicates the status of current check
    """
    logging.append("INFO: Start to run sanity checks for user spreadsheet data.")

    # Case 1: removes NA rows in index
    user_spreadsheet_df_idx_na_rmd = remove_na_index(user_spreadsheet_df)
    if user_spreadsheet_df_idx_na_rmd is None:
        return None

    # Case 2: checks the duplication on column name and removes it if exists
    user_spreadsheet_df_col_dedup = check_duplicate_column_name(user_spreadsheet_df_idx_na_rmd)
    if user_spreadsheet_df_col_dedup is None:
        return None

    # Case 3: checks the duplication on gene name and removes it if exists
    user_spreadsheet_df_genename_dedup = check_duplicate_row_name(user_spreadsheet_df_col_dedup)
    if user_spreadsheet_df_genename_dedup is None:
        return None

    # Case 4: checks the validity of gene name to see if it can be ensemble or not
    user_spreadsheet_df_final = check_ensemble_gene_name(user_spreadsheet_df_genename_dedup, run_parameters)

    logging.append("INFO: Finished running sanity check for user spreadsheet data.")

    return user_spreadsheet_df_final


def check_intersection(list_a, list_b):
    '''
    Find intersection between list_a, list_b
    Args:
        list_a: list a
        list_b: list b

    Returns:
        intersection: the intersection
    '''
    intersection = list(set(list_a) & set(list_b))
    if not intersection:
        logging.append("ERROR: Cannot find intersection between spreadsheet and phenotype data.")
        return None
    logging.append("INFO: Found {} intersections between phenotype and spreadsheet data.".format(len(intersection)))
    return intersection


def run_pre_processing_phenotype_data(phenotype_df, user_spreadsheet_df_header):
    '''
    Pre-processing phenotype data. This includes checking for na index, duplicate column name and row name.
    Args:
        phenotype_df: input phenotype dataframe to be checked

    Returns:
        phenotype_df_genename_dedup: cleaned phenotype dataframe
    '''
    logging.append("INFO: Start to run sanity check for phenotype data.")

    # Case 1: removes NA rows in index
    phenotype_df_idx_na_rmd = remove_na_index(phenotype_df)
    if phenotype_df_idx_na_rmd is None:
        return None

    # Case 2: checks the duplication on column name and removes it if exists
    phenotype_df_col_dedup = check_duplicate_column_name(phenotype_df_idx_na_rmd)
    if phenotype_df_col_dedup is None:
        return None

    # Case 3: checks the duplication on row name and removes it if exists
    phenotype_df_genename_dedup = check_duplicate_row_name(phenotype_df_col_dedup)
    if phenotype_df_genename_dedup is None:
        return None

    # Case 4: checks the intersection on phenotype
    intersection = check_intersection(phenotype_df_genename_dedup.index.values, user_spreadsheet_df_header)
    if intersection is None:
        return None

    logging.append("INFO: Finished running sanity check for phenotype data.")

    return phenotype_df_genename_dedup


def generate_logging(flag, message, path):
    '''
    Creates logging file
    Args:
        flag: a boolean value indicating if the current run is succeeded or not.
        message: a list of error message
        path: log file location

    Returns:
        NA

    '''
    import yaml
    if flag:
        status = "SUCCESS"
    else:
        status = "FAIL"
    file_content = {status: message}
    output_stream = open(path, "w")
    yaml.dump(file_content, output_stream, default_flow_style=False)
    # reset the global logging list
    del logging[:]
    output_stream.close()
