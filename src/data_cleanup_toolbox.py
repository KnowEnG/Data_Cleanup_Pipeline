"""
    This module serves as a connecting function between front end and back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. 
"""
import pandas
import redis_utilities as redisutil
from knpackage.toolbox import get_network_df, extract_network_node_names, find_unique_node_names
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

    if user_spreadsheet_df is None:
        return False, logging

    # Value check logic a: checks if only 0 and 1 appears in user spreadsheet and rename phenotype data file to have _ETL.tsv suffix
    user_spreadsheet_val_chked = check_input_value_for_gsc_sc_common(user_spreadsheet_df)

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
        "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df_cleaned.shape[0],
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
    if user_spreadsheet_df is None:
        return False, logging

    phenotype_df_cleaned = None
    if 'phenotype_name_full_path' in run_parameters.keys():
        logging.append("INFO: Start to process phenotype data.")
        phenotype_df = load_data_file(run_parameters['phenotype_name_full_path'])
        if phenotype_df is None:
            return False, logging
        else:
            phenotype_df_cleaned = run_pre_processing_phenotype_data(phenotype_df, user_spreadsheet_df.columns.values)
            if phenotype_df_cleaned is None:
                return False, logging

    logging.append("INFO: Start to process user spreadsheet data.")
    # Value check logic a: checks if only real number appears in user spreadsheet and create absolute value
    user_spreadsheet_val_chked = check_input_value_for_gsc_sc_common(user_spreadsheet_df)

    if user_spreadsheet_val_chked is None:
        return False, logging

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if 'gg_network_name_full_path' in run_parameters.keys():
        logging.append("INFO: Start to process network data.")
        # Loads network dataframe to check number of genes intersected between spreadsheet and network
        network_df = get_network_df(run_parameters['gg_network_name_full_path'])
        if network_df.empty:
            logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
                run_parameters['gg_network_name_full_path']))
            return False, logging
        node_1_names, node_2_names = extract_network_node_names(network_df)
        unique_gene_names = find_unique_node_names(node_1_names, node_2_names)

        intersection = find_intersection(unique_gene_names, user_spreadsheet_df_cleaned.index)
        if intersection is None:
            logging.append('ERROR: Cannot find intersection between spreadsheet genes and network genes.')
            return False, logging

    # The logic here ensures that even if phenotype data doesn't fits requirement, the rest pipelines can still run.
    if user_spreadsheet_df_cleaned is None:
        return False, logging
    else:
        user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                           sep='\t', header=True, index=True)
        logging.append(
            "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df_cleaned.shape[0],
                                                                                 user_spreadsheet_df_cleaned.shape[1]))
    if phenotype_df_cleaned is not None:
        phenotype_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['phenotype_name_full_path']) + "_ETL.tsv",
                                    sep='\t', header=True, index=True)
        logging.append(
            "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_df_cleaned.shape[0],
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
    # dimension: sample x phenotype
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])

    if user_spreadsheet_df is None:
        return False, logging

    # dimension: sample x phenotype
    phenotype_df = load_data_file(run_parameters['phenotype_name_full_path'])

    if phenotype_df is None:
        return False, logging

    # Value check logic b: checks if only 0 and 1 appears in user spreadsheet or if satisfies certain criteria
    user_spreadsheet_val_chked, phenotype_val_checked = check_input_value_for_gene_prioritization(
        user_spreadsheet_df, phenotype_df, run_parameters['correlation_measure'])

    if user_spreadsheet_val_chked is None or phenotype_val_checked is None:
        return False, logging

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if user_spreadsheet_df_cleaned is None or phenotype_val_checked is None:
        return False, logging

    # stores cleaned phenotype data (transposed) to a file, dimension: phenotype x sample
    phenotype_val_checked.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
        run_parameters['phenotype_name_full_path']) + "_ETL.tsv",
                                 sep='\t', header=True, index=True)
    user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
        run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                       sep='\t', header=True, index=True)
    logging.append(
        "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df_cleaned.shape[0],
                                                                             user_spreadsheet_df_cleaned.shape[1]))
    logging.append(
        "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_val_checked.shape[0],
                                                                           phenotype_val_checked.shape[1]))
    return True, logging


def remove_empty_row(dataframe):
    '''
    Remove empty rows in a dataframe
    Args:
        dataframe: input dataframe

    Returns:
        a dataframe without empty line

    '''
    org_row_cnt = dataframe.shape[0]
    dataframe_no_empty_line = dataframe.dropna(how='all')
    new_row_cnt = dataframe_no_empty_line.shape[0]
    diff = org_row_cnt - new_row_cnt
    if diff > 0:
        logging.append("WARNING: Removed {} empty row(s).".format(diff))
    if dataframe_no_empty_line.empty:
        logging.append(
            "ERROR: After removed {} empty row(s), original dataframe in shape ({},{}) "
            "becames empty.".format(diff, dataframe.shape[0], dataframe.shape[1]))
        return None
    return dataframe_no_empty_line


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

    if diff > 0 and org_row_cnt == new_row_cnt:
        logging.append(
            "ERROR: After removed {} row(s) that contains NA in index, original dataframe "
            "in shape ({},{}) becames empty.".format(
                diff, dataframe.shape[0], dataframe.shape[1]))
        return None

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
    if not file_path or not file_path.strip():
        logging.append("ERROR: Input file path is empty: {}. Please provide a valid input path.".format(file_path))
        return None

    try:
        # loads input data
        input_df = pandas.read_csv(file_path, sep='\t', index_col=0, header=0, mangle_dupe_cols=False,
                                   error_bad_lines=False, warn_bad_lines=True)

        # casting index and columns to String type
        input_df.index = input_df.index.map(str)
        input_df.columns = input_df.columns.map(str)

        logging.append("INFO: Successfully loaded input data: {} with {} row(s) and {} "
                       "column(s)".format(file_path, input_df.shape[0], input_df.shape[1]))

        # removes empty rows
        input_df_wo_empty_ln = remove_empty_row(input_df)
        if input_df_wo_empty_ln is None or input_df_wo_empty_ln.empty:
            logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(file_path))
            return None

        return input_df_wo_empty_ln
    except Exception as err:
        logging.append("ERROR: {}".format(str(err)))
        return None


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


def check_phenotype_data_for_gene_prioritization(data_frame_header, phenotype_df_pxs, correlation_measure):
    """
    Phenotype data check for gene_prioritization_pipeline
    1. checks intersection between phenotype data and user spreadsheet on each drug
    2. verifies data value for t-test and pearson separately
    Args:
        data_frame_header: user spreadsheet
        phenotype_df_pxs: phenotype data
        correlation_measure: correlation measure: pearson or t-test

    Returns:
        phenotype_df_pxs_trimmed: trimmed phenotype data

    """
    # a list to store headers that has intersection between phenotype data and user spreadsheet
    valid_samples = []

    # loop through phenotype (phenotype x sample) to check header intersection between phenotype and spreadsheet
    for column in range(0, len(phenotype_df_pxs.columns)):
        # drops columns with NA value in phenotype dataframe
        phenotype_df_sxp = phenotype_df_pxs.ix[:, column].to_frame().dropna(axis=0)
        phenotype_index = list(phenotype_df_sxp.index.values)
        # finds common headers
        common_headers = set(phenotype_index) & set(data_frame_header)
        cur_column_name = phenotype_df_pxs.columns[column]
        if not common_headers:
            logging.append(
                "WARNING: Cannot find intersection on phenotype between user spreadsheet and "
                "phenotype data on column: {}. Removing it now.".format(cur_column_name))
        elif len(common_headers) < 2:
            logging.append(
                "WARNING: Number of samples is too small to run further tests (Pearson, t-test) "
                "on column: {}. Removing it now.".format(cur_column_name))
        else:
            valid_samples.append(phenotype_df_pxs.columns[column])

    if len(valid_samples) == 0:
        logging.append("ERROR: Cannot find any valid column in phenotype data "
                       "that has intersection with spreadsheet data.")
        return None

    # remove the columns that doesn't contain intersections in phenotype data
    phenotype_df_pxs_trimmed = phenotype_df_pxs[sorted(valid_samples)]

    # defines the default values that can exist in phenotype data
    gold_value_set = {0, 1}
    if correlation_measure == 't_test':
        list_values = pandas.unique(phenotype_df_pxs_trimmed.values.ravel())
        phenotype_value_set = set(filter(lambda x: x == x, list_values))
        if gold_value_set != phenotype_value_set:
            logging.append(
                "ERROR: Only 0, 1 are allowed in phenotype data when running t_test. "
                "Please revise your phenotype data and reupload.")
            return None

    if correlation_measure == 'pearson':
        if False in phenotype_df_pxs_trimmed.applymap(lambda x: isinstance(x, (int, float))):
            logging.append(
                "ERROR: Only numeric value is allowed in phenotype data when running pearson test. "
                "Found non-numeric value in phenotype data.")
            return None

    return phenotype_df_pxs_trimmed


def check_input_value_for_gene_prioritization(data_frame, phenotype_df, correlation_measure):
    """
    Input data check for Gene_Prioritization_Pipelien
    Args:
        data_frame: original data frame generated by user spreadsheet
        phenotype_df: original phenotype data
        correlation_measure: correlation measure : pearson or t-test

    Returns:
        data_frame_dropna: cleaned user spreadsheet
        phenotype_df_pxs: phenotype data

    """
    # drops column which contains NA in data_frame to reduce phenotype dimension
    data_frame_dropna = data_frame.dropna(axis=1)

    if data_frame_dropna.empty:
        logging.append("ERROR: User spreadsheet is empty after removing NA.")
        return None, None

    # checks real number negative to positive infinite
    if False in data_frame_dropna.applymap(lambda x: isinstance(x, (int, float))).values:
        logging.append("ERROR: Found non-numeric value in user spreadsheet.")
        return None, None

    logging.append("INFO: Start to run checks for phenotypic data.")

    # output dimension: sample x phenotype
    data_frame_header = list(data_frame_dropna.columns.values)

    phenotype_df_pxs = check_phenotype_data_for_gene_prioritization(data_frame_header, phenotype_df,
                                                                    correlation_measure)
    logging.append("INFO: Finished running checks for phenotypic data.")

    return data_frame_dropna, phenotype_df_pxs


def check_input_value_for_gsc_sc_common(data_frame):
    """
    Checks if the values in user spreadsheet passed the following criteria:
    1. no None value
    2. only real number is allowed
    3. no negative value

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

    check_value = data_frame.applymap(lambda x: isinstance(x, (int, float))).values

    # checks if user spreadsheet contains only real number
    if False in data_frame.applymap(lambda x: isinstance(x, (int, float))).values:
        logging.append("ERROR: Found non-numeric value in user spreadsheet.")
        return None

    # checks if user spreadsheet contains only non-negative number
    if False in data_frame.applymap(lambda x: x >= 0).values:
        logging.append("ERROR: Found negative value in user spreadsheet.")
        return None

    return data_frame


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

    # copy index to new column named with 'original'
    data_frame = data_frame.assign(original=data_frame.index)
    redis_ret = redisutil.get_node_info(redis_db, data_frame.index, "Gene", run_parameters['source_hint'],
                                        run_parameters['taxonid'])

    # extract ensemble names as a list from a call to redis database
    ensemble_names = [x[1] for x in redis_ret]
    # resets data_frame's index with ensembel name
    data_frame.index = pandas.Series(ensemble_names)

    # extracts all mapped rows in dataframe
    output_df_mapped = data_frame[~data_frame.index.str.contains(r'^unmapped.*$')]
    output_df_mapped = output_df_mapped.drop('original', axis=1)
    output_df_mapped_dedup = output_df_mapped[~output_df_mapped.index.duplicated()]

    dup_cnt = output_df_mapped.shape[0] - output_df_mapped_dedup.shape[0]
    if dup_cnt > 0:
        logging.append("INFO: Found {} duplicate Ensembl gene name.".format(dup_cnt))

    # dedup on gene name mapping dictionary
    mapping = data_frame[['original']]

    mapping_filtered = mapping[~mapping.index.str.contains(r'^unmapped.*$')]

    logging.append("INFO: Mapped {} gene(s) to ensemble name.".format(mapping_filtered.shape[0]))

    unmapped_filtered = mapping[mapping.index.str.contains(r'^unmapped.*$')].sort_index(axis=0, ascending=False)
    unmapped_filtered['ensemble'] = unmapped_filtered.index

    if unmapped_filtered.shape[0] > 0:
        logging.append("INFO: Unable to map {} gene(s) to ensemble name.".format(unmapped_filtered.shape[0]))

    mapping_dedup_df = mapping_filtered[~mapping_filtered.index.duplicated()]

    output_file_basename = get_file_basename(run_parameters['spreadsheet_name_full_path'])

    # writes unmapped gene name along with return value from Redis database to a file
    unmapped_filtered.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_UNMAPPED.tsv",
                             sep='\t', header=False, index=False)

    # writes dedupped mapping between original gene name and ensemble name to a file
    mapping_dedup_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_MAP.tsv",
                            sep='\t', header=False, index=True)

    if output_df_mapped_dedup.empty:
        logging.append("ERROR: No valid ensemble name can be found.")
        return None

    return output_df_mapped_dedup


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


def find_intersection(list_a, list_b):
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
    logging.append(
        "INFO: Found {} intersected gene(s) between phenotype and spreadsheet data.".format(len(intersection)))
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
    intersection = find_intersection(phenotype_df_genename_dedup.index.values, user_spreadsheet_df_header)
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
