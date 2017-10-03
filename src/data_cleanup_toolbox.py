"""
    This module serves as a connecting function between front end and back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. 
"""
import os

import pandas
import redis_utilities as redisutil
from knpackage.toolbox import get_network_df, extract_network_node_names, find_unique_node_names, get_spreadsheet_df

logging = []


def run_geneset_characterization_pipeline(run_parameters):
    """
    Runs data cleaning for geneset_characterization_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not.
        message: A message indicates the status of current check.
    """
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])

    if user_spreadsheet_df is None:
        return False, logging

    # Value check logic a: checks if only non-negative value appears in user spreadsheet
    user_spreadsheet_val_chked = check_not_null_non_negative_real_value(user_spreadsheet_df)

    if user_spreadsheet_val_chked is None:
        return False, logging

    # Checks duplication on column and row name
    user_spreadsheet_df_checked = sanity_check_input_data(user_spreadsheet_val_chked)

    # Checks the validity of gene name to see if it can be ensemble or not
    user_spreadsheet_df_cleaned = map_ensemble_gene_name(user_spreadsheet_df_checked, run_parameters)

    if user_spreadsheet_df_cleaned is None:
        return False, logging

    write_to_file(user_spreadsheet_df_cleaned, run_parameters['spreadsheet_name_full_path'],
                  run_parameters['results_directory'], "_ETL.tsv")
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
        validation_flag: Boolean type value indicating if input data is valid or not.
        message: A message indicates the status of current check.
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
    user_spreadsheet_val_chked = check_not_null_non_negative_real_value(user_spreadsheet_df)

    if user_spreadsheet_val_chked is None:
        return False, logging

    # Checks duplication on column and row name
    user_spreadsheet_df_checked = sanity_check_input_data(user_spreadsheet_val_chked)

    # Checks the validity of gene name to see if it can be ensemble or not
    user_spreadsheet_df_cleaned = map_ensemble_gene_name(user_spreadsheet_df_checked, run_parameters)

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
        write_to_file(user_spreadsheet_df_cleaned, run_parameters['spreadsheet_name_full_path'],
                      run_parameters['results_directory'], "_ETL.tsv")
        logging.append(
            "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df_cleaned.shape[0],
                                                                                 user_spreadsheet_df_cleaned.shape[1]))
    if phenotype_df_cleaned is not None:
        write_to_file(phenotype_df_cleaned, run_parameters['phenotype_name_full_path'],
                      run_parameters['results_directory'], "_ETL.tsv")
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
        validation_flag: Boolean type value indicating if input data is valid or not.
        message: A message indicates the status of current check.
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

    # Checks duplication on column and row name
    user_spreadsheet_df_checked = sanity_check_input_data(user_spreadsheet_val_chked)

    # Checks the validity of gene name to see if it can be ensemble or not
    user_spreadsheet_df_cleaned = map_ensemble_gene_name(user_spreadsheet_df_checked, run_parameters)

    if user_spreadsheet_df_cleaned is None or phenotype_val_checked is None:
        return False, logging

    # Stores cleaned phenotype data (transposed) to a file, dimension: phenotype x sample
    write_to_file(phenotype_val_checked, run_parameters['phenotype_name_full_path'],
                  run_parameters['results_directory'], "_ETL.tsv")
    write_to_file(user_spreadsheet_df_cleaned, run_parameters['spreadsheet_name_full_path'],
                  run_parameters['results_directory'], "_ETL.tsv")

    logging.append(
        "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df_cleaned.shape[0],
                                                                             user_spreadsheet_df_cleaned.shape[1]))
    logging.append(
        "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_val_checked.shape[0],
                                                                           phenotype_val_checked.shape[1]))
    return True, logging


def run_phenotype_prediction_pipeline(run_parameters):
    """
        Runs data cleaning for phenotype_prediction_pipeline.

        Args:
            run_parameters: configuration dictionary

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
    """
    # dimension: sample x phenotype
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])
    if user_spreadsheet_df is None:
        return False, logging

    # dimension: sample x phenotype
    phenotype_df = load_data_file(run_parameters['phenotype_name_full_path'])

    if phenotype_df is None:
        return False, logging

    user_spreadsheet_dropna = check_real_value_dropna_colwise(user_spreadsheet_df)

    if user_spreadsheet_dropna is None or user_spreadsheet_dropna.empty:
        logging.append("ERROR: After drop NA, user spreadsheet data becomes empty.")
        return None, None

    # Checks if there is valid intersection between phenotype data and user spreadsheet data
    dataframe_header = list(user_spreadsheet_dropna.columns.values)
    phenotype_df_pxs_trimmed = check_intersection_for_phenotype_and_user_spreadsheet(dataframe_header, phenotype_df)

    # Checks duplication on column and row name
    user_spreadsheet_df_cleaned = sanity_check_input_data(user_spreadsheet_dropna)
    if user_spreadsheet_df_cleaned is None or phenotype_df_pxs_trimmed is None:
        return False, logging

    # Stores cleaned phenotype data (transposed) to a file, dimension: phenotype x sample
    write_to_file(phenotype_df_pxs_trimmed, run_parameters['phenotype_name_full_path'],
                  run_parameters['results_directory'], "_ETL.tsv")
    write_to_file(user_spreadsheet_df_cleaned, run_parameters['spreadsheet_name_full_path'],
                  run_parameters['results_directory'], "_ETL.tsv")

    logging.append(
        "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df_cleaned.shape[0],
                                                                             user_spreadsheet_df_cleaned.shape[1]))
    logging.append(
        "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_df_pxs_trimmed.shape[0],
                                                                           phenotype_df_pxs_trimmed.shape[1]))
    return True, logging


def run_general_clustering_pipeline(run_parameters):
    """
        Runs data cleaning for general_clustering_pipeline.

        Args:
            run_parameters: configuration dictionary

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
    """
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])

    if user_spreadsheet_df is None:
        return False, logging

    user_spreadsheet_df_val_check = check_not_null_real_value(user_spreadsheet_df)
    if user_spreadsheet_df_val_check is None:
        return False, logging

    user_spreadsheet_df_rm_na_header = remove_na_header(user_spreadsheet_df_val_check)
    if user_spreadsheet_df_rm_na_header is None:
        return False, logging

    user_spreadsheet_df_cleaned = sanity_check_input_data(user_spreadsheet_df_rm_na_header)
    if user_spreadsheet_df_cleaned is None:
        return False, logging

    write_to_file(user_spreadsheet_df_cleaned, run_parameters['spreadsheet_name_full_path'],
                  run_parameters['results_directory'], "_ETL.tsv")
    logging.append(
        "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df_cleaned.shape[0],
                                                                             user_spreadsheet_df_cleaned.shape[1]))
    return True, logging


def run_pasted_gene_set_conversion(run_parameters):
    """
    Runs data cleaning for pasted_gene_set_conversion.

    Args:
        run_parameters:

    Returns:

    """
    # gets redis database instance by its credential
    redis_db = redisutil.get_database(run_parameters['redis_credential'])

    # reads pasted_gene_list as a dataframe
    input_small_genes_df = get_spreadsheet_df(run_parameters['pasted_gene_list_full_path'])
    logging.append("INFO: Successfully load spreadsheet data: {} with {} gene(s).".format(
        run_parameters['pasted_gene_list_full_path'], input_small_genes_df.shape[0]))

    # removes nan index rows
    input_small_genes_df = remove_na_index(input_small_genes_df)

    # casting index to String type
    input_small_genes_df.index = input_small_genes_df.index.map(str)

    if input_small_genes_df is None or len(input_small_genes_df.index) == 0:
        logging.append("ERROR: Input data is empty. Please upload valid input data.")
        return False, logging

    input_small_genes_df["original_gene_name"] = input_small_genes_df.index
    # converts pasted_gene_list to ensemble name
    redis_ret = redisutil.get_node_info(redis_db, input_small_genes_df.index, "Gene", run_parameters['source_hint'],
                                        run_parameters['taxonid'])
    ensemble_names = [x[1] for x in redis_ret]
    input_small_genes_df.index = pandas.Series(ensemble_names)
    # filters out the unmapped genes
    mapped_small_genes_df = input_small_genes_df[~input_small_genes_df.index.str.contains(r'^unmapped.*$')]
    # reads the univeral_gene_list
    universal_genes_df = get_spreadsheet_df(run_parameters['temp_redis_vector'])
    # inserts a column with value 0
    universal_genes_df.insert(0, 'value', 0)
    # finds the intersection between pasted_gene_list and universal_gene_list
    common_idx = universal_genes_df.index.intersection(mapped_small_genes_df.index)
    logging.append("INFO: Found {} common gene(s) that shared between pasted gene list and universal gene list.".format(
        len(common_idx)))
    # inserts a column with value 1
    universal_genes_df.loc[common_idx] = 1
    # names the column of universal_genes_df to be 'uploaded_gene_set'
    universal_genes_df.columns = ["uploaded_gene_set"]
    del universal_genes_df.index.name

    # outputs final results
    write_to_file(mapped_small_genes_df, run_parameters['pasted_gene_list_full_path'],
                  run_parameters['results_directory'], "_MAP.tsv")
    write_to_file(universal_genes_df, run_parameters['pasted_gene_list_full_path'],
                  run_parameters['results_directory'], "_ETL.tsv")
    logging.append("INFO: Universal gene list contains {} genes.".format(universal_genes_df.shape[0]))
    logging.append("INFO: Mapped gene list contains {} genes.".format(mapped_small_genes_df.shape[0]))
    return True, logging


def run_signature_analysis_pipeline(run_parameters):
    """
    Runs data cleaning for signature_analysis_pipeline.

    Args:
        run_parameters:

    Returns:

    """
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])
    if user_spreadsheet_df is None:
        return False, logging

    # dimension: sample x phenotype
    phenotype_df = load_data_file(run_parameters['phenotype_name_full_path'])
    if phenotype_df is None:
        return False, logging

    if 'gg_network_name_full_path' in run_parameters.keys():
        logging.append("INFO: Start to process network data.")
        # Loads network dataframe to check number of genes intersected between spreadsheet and network
        network_df = get_network_df(run_parameters['gg_network_name_full_path'])
        if network_df.empty:
            logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
                run_parameters['gg_network_name_full_path']))
            return False, logging



def run_feature_prioritization_pipeline(run_parameters):
    """

    Args:
        run_parameters:

    Returns:

    """
    user_spreadsheet_df = load_data_file(run_parameters['spreadsheet_name_full_path'])
    if user_spreadsheet_df is None:
        return False, logging

    # dimension: sample x phenotype
    phenotype_df = load_data_file(run_parameters['phenotype_name_full_path'])
    if phenotype_df is None:
        return False, logging

    user_spreadsheet_df_val_check = check_not_null_real_value(user_spreadsheet_df)
    if user_spreadsheet_df_val_check is None:
        return False, logging

    phenotype_df_val_check = check_data_for_t_test_and_pearson(phenotype_df, run_parameters['correlation_measure'])
    if phenotype_df_val_check is None:
        return False, logging

    write_to_file(user_spreadsheet_df, run_parameters['spreadsheet_name_full_path'],
                      run_parameters['results_directory'], "_ETL.tsv")
    logging.append("INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(user_spreadsheet_df.shape[0],
                                                                         user_spreadsheet_df.shape[1]))
    write_to_file(phenotype_df_val_check, run_parameters['phenotype_name_full_path'], run_parameters['results_directory'], "_ETL.tsv")
    logging.append("INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_df_val_check.shape[0],
                                                                                    phenotype_df_val_check.shape[1]))

def remove_empty_row(dataframe):
    """
    Remove empty rows in a dataframe
    Args:
        dataframe: input dataframe

    Returns:
        a dataframe without empty line
    """
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
    dataframe_rm_nan_idx = dataframe[dataframe.index != "nan"]
    dataframe_rm_null_idx = dataframe_rm_nan_idx[dataframe_rm_nan_idx.index != None]
    new_row_cnt = dataframe_rm_null_idx.shape[0]
    diff = org_row_cnt - new_row_cnt

    if diff > 0:
        logging.append("WARNING: Removed {} row(s) which contains NA in index.".format(diff))

    if diff > 0 and new_row_cnt == 0:
        logging.append(
            "ERROR: After removed {} row(s) that contains NA in index, original dataframe "
            "in shape ({},{}) becames empty.".format(
                diff, dataframe.shape[0], dataframe.shape[1]))
        return None

    return dataframe_rm_null_idx


def remove_na_header(dataframe):
    """
    Remove NA in header

    Args:
        dataframe: input DataFrame

    Returns:
        dataframe: with not null header selected
    """
    header = list(dataframe.columns.values)
    header_rm_na = [col for col in header if col is not None]
    diff = len(header) - len(header_rm_na)
    dataframe_rm_na_header = dataframe[header_rm_na]

    if diff > 0:
        logging.append("WARNING: Removed {} column(s) which contains NA in header.".format(diff))

    if diff > 0 and dataframe_rm_na_header.empty:
        logging.append("ERROR: After removed {} column(s) that contains NA in header, original dataframe "
                       "in shape ({},{}) becames empty.".format(diff, dataframe.shape[0], dataframe.shape[1]))
        return None

    return dataframe_rm_na_header


def write_to_file(target_file, target_path, result_directory, suffix, use_index=True, use_header=True):
    """
    Write to a csv file.

    Args:
        target_file: the file will be write to disk
        target_path: the path of the original target_file
        result_directory: target file directory
        suffix: output file suffix

    Returns:
        NA
    """
    output_file_basename = os.path.splitext(os.path.basename(os.path.normpath(target_path)))[0]
    target_file.to_csv(result_directory + '/' + output_file_basename + suffix,
                       sep='\t', index=use_index, header=use_header)


def load_data_file(file_path):
    """
    Loads data file as a DataFrame object by a given file path.

    Args:
        file_path: input file, which is uploaded from frontend

    Returns:
        input_df_wo_empty_ln: user input as a DataFrame, which doesn't have empty line
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


def check_duplicate_column_name(dataframe):
    """
    Checks duplicate column names and rejects it if it exists

    Args:
        dataframe: input dataframe to be checked

    Returns:
        user_spreadsheet_df_genename_dedup.T: a DataFrame in original format
        ret_msg: error message
    """
    dataframe_transpose = dataframe.T
    dataframe_row_dedup = dataframe_transpose[~dataframe_transpose.index.duplicated()]
    if dataframe_row_dedup.empty:
        logging.append("ERROR: User spreadsheet becomes empty after remove column duplicates.")
        return None

    row_count_diff = len(dataframe_transpose.index) - len(dataframe_row_dedup.index)

    if row_count_diff > 0:
        logging.append("WARNING: Removed {} duplicate column(s) from user spreadsheet.".format(row_count_diff))
        return dataframe_row_dedup.T

    if row_count_diff == 0:
        logging.append("INFO: No duplicate column name detected in this data set.")
        return dataframe_row_dedup.T

    if row_count_diff < 0:
        logging.append("ERROR: An unexpected error occurred during checking duplicate column name.")
        return None


def check_duplicate_row_name(dataframe):
    """
    Checks duplication on gene name and rejects it if it exists.

    Args:
        dataframe: input DataFrame

    Returns:
        dataframe_genename_dedup: a DataFrame in original format
        ret_msg: error message
    """
    dataframe_genename_dedup = dataframe[~dataframe.index.duplicated()]
    if dataframe_genename_dedup.empty:
        logging.append("ERROR: User spreadsheet becomes empty after remove column duplicates.")
        return None

    row_count_diff = len(dataframe.index) - len(dataframe_genename_dedup.index)
    if row_count_diff > 0:
        logging.append("WARNING: Removed {} duplicate row(s) from user spreadsheet.".format(row_count_diff))
        return dataframe_genename_dedup

    if row_count_diff == 0:
        logging.append("INFO: No duplicate row name detected in this data set.")
        return dataframe_genename_dedup

    if row_count_diff < 0:
        logging.append("ERROR: An unexpected error occurred during checking duplicate row name.")
        return None


def check_intersection_for_phenotype_and_user_spreadsheet(dataframe_header, phenotype_df_pxs):
    '''
    Checks intersection between phenotype data and user spreadsheet on each drug

    Args:
        dataframe_header: the header of dataframe as a list
        phenotype_df_pxs: phenotype dataframe in phenotype x sample

    Returns:
        phenotype_df_pxs_trimmed: a trimmed phenotype dataframe

    '''
    # a list to store headers that has intersection between phenotype data and user spreadsheet
    valid_samples = []

    # loop through phenotype (phenotype x sample) to check header intersection between phenotype and spreadsheet
    for column in range(0, len(phenotype_df_pxs.columns)):
        # drops columns with NA value in phenotype dataframe
        phenotype_df_sxp = phenotype_df_pxs.ix[:, column].to_frame().dropna(axis=0)
        phenotype_index = list(phenotype_df_sxp.index.values)
        # finds common headers
        common_headers = set(phenotype_index) & set(dataframe_header)
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

    return phenotype_df_pxs_trimmed


def check_data_for_t_test_and_pearson(phenotype_df_pxs, correlation_measure):
    """
    Verifies data value for t-test and pearson separately.

    Args:
        phenotype_df_pxs: phenotype data
        correlation_measure: correlation measure: pearson or t-test

    Returns:
        phenotype_df_pxs: cleaned phenotype data

    """
    # defines the default values that can exist in phenotype data
    gold_value_set = {0, 1}
    if correlation_measure == 't_test':
        list_values = pandas.unique(phenotype_df_pxs.values.ravel())
        phenotype_value_set = set(filter(lambda x: x == x, list_values))
        if gold_value_set != phenotype_value_set:
            logging.append(
                "ERROR: Only 0, 1 are allowed in phenotype data when running t_test. "
                "Please revise your phenotype data and reupload.")
            return None

    if correlation_measure == 'pearson':
        if False in phenotype_df_pxs.applymap(lambda x: isinstance(x, (int, float))):
            logging.append(
                "ERROR: Only numeric value is allowed in phenotype data when running pearson test. "
                "Found non-numeric value in phenotype data.")
            return None

    return phenotype_df_pxs


def check_real_value_dropna_colwise(dataframe):
    """
    User spreadsheet data check :
        1. remove na in column wise
        2. check if DataFrame is empty
        3. check if DataFrame contains only real value

    Args:
        dataframe: the original DataFrame to be cleaned

    Returns:
        dataframe_dropna: a cleaned DataFrame

    """
    # drops column which contains NA in dataframe to reduce phenotype dimension
    dataframe_dropna = dataframe.dropna(axis=1)
    if dataframe.shape[1] - dataframe_dropna.shape[1] > 0:
        logging.append(
            "INFO: Remove {} column(s) which contains NA.".format(dataframe.shape[1] - dataframe_dropna.shape[1]))

    if dataframe_dropna.empty:
        logging.append("ERROR: User spreadsheet is empty after removing NA.")
        return None

    # checks real number negative to positive infinite
    if False in dataframe_dropna.applymap(lambda x: isinstance(x, (int, float))).values:
        logging.append("ERROR: Found non-numeric value in user spreadsheet.")
        return None

    return dataframe_dropna


def check_input_value_for_gene_prioritization(user_spreadsheet_df, phenotype_df, correlation_measure):
    """
    Input data check for Gene_Prioritization_Pipeline.

    Args:
        dataframe: original DataFrame generated by user spreadsheet
        phenotype_df: original phenotype data
        correlation_measure: correlation measure : pearson or t-test

    Returns:
        dataframe_dropna: cleaned user spreadsheet
        phenotype_df_pxs: phenotype data

    """

    user_spreadsheet_df_dropna = check_real_value_dropna_colwise(user_spreadsheet_df)
    logging.append("INFO: Start to run checks for phenotypic data.")

    if user_spreadsheet_df_dropna is None or user_spreadsheet_df_dropna.empty:
        logging.append("ERROR: After drop NA, user spreadsheet data becomes empty.")
        return None, None

    # output dimension: sample x phenotype
    user_spreadsheet_df_header = list(user_spreadsheet_df_dropna.columns.values)

    phenotype_df_pxs_trimmed = check_intersection_for_phenotype_and_user_spreadsheet(user_spreadsheet_df_header,
                                                                                     phenotype_df)

    if phenotype_df_pxs_trimmed is None or phenotype_df_pxs_trimmed.empty:
        logging.append("ERROR: After drop NA, phenotype data becomes empty.")
        return None, None
    phenotype_df_pxs = check_data_for_t_test_and_pearson(phenotype_df_pxs_trimmed, correlation_measure)
    logging.append("INFO: Finished running checks for phenotypic data.")

    return user_spreadsheet_df_dropna, phenotype_df_pxs


def check_not_null_real_value(dataframe):
    """
    Checks if the values in user spreadsheet passed the following criteria:
       1. no None value
       2. only real number is allowed

    Args:
        dataframe: input DataFrame

    Returns:
        dataframe: processed dataframe
       """
    if dataframe.isnull().values.any():
        logging.append("ERROR: This user spreadsheet contains invalid NaN value.")
        return None

    # checks if user spreadsheet contains only real number
    if False in dataframe.applymap(lambda x: isinstance(x, (int, float))).values:
        logging.append("ERROR: Found non-numeric value in user spreadsheet.")
        return None

    return dataframe


def check_not_null_non_negative_real_value(dataframe):
    """
    Checks if the values in user spreadsheet passed the following criteria:
        1. no None value
        2. only real number is allowed
        3. no negative value

    Args:
        dataframe: input DataFrame

    Returns:
        dataframe: processed dataframe
    """
    if dataframe.isnull().values.any():
        logging.append("ERROR: This user spreadsheet contains invalid NaN value.")
        return None

    # checks if user spreadsheet contains only real number
    if False in dataframe.applymap(lambda x: isinstance(x, (int, float))).values:
        logging.append("ERROR: Found non-numeric value in user spreadsheet.")
        return None

    # checks if user spreadsheet contains only non-negative number
    if False in dataframe.applymap(lambda x: x >= 0).values:
        logging.append("ERROR: Found negative value in user spreadsheet.")
        return None

    return dataframe


def map_ensemble_gene_name(dataframe, run_parameters):
    """
    Checks if the gene name follows ensemble format.

    Args:
        dataframe: input DataFrame
        run_parameters: user configuration from run_file

    Returns:
         output_df_mapped_dedup: cleaned DataFrame
    """
    redis_db = redisutil.get_database(run_parameters['redis_credential'])

    # copy index to new column named with 'original'
    dataframe = dataframe.assign(original=dataframe.index)
    redis_ret = redisutil.get_node_info(redis_db, dataframe.index, "Gene", run_parameters['source_hint'],
                                        run_parameters['taxonid'])

    # extract ensemble names as a list from a call to redis database
    ensemble_names = [x[1] for x in redis_ret]

    # resets dataframe's index with ensembel name
    dataframe.index = pandas.Series(ensemble_names)

    # extracts all mapped rows in dataframe
    output_df_mapped = dataframe[~dataframe.index.str.contains(r'^unmapped.*$')]
    if output_df_mapped.empty:
        logging.append("ERROR: No valid ensemble name can be found.")
        return None

    output_df_mapped = output_df_mapped.drop('original', axis=1)
    output_df_mapped_dedup = output_df_mapped[~output_df_mapped.index.duplicated()]

    dup_cnt = output_df_mapped.shape[0] - output_df_mapped_dedup.shape[0]
    if dup_cnt > 0:
        logging.append("INFO: Found {} duplicate Ensembl gene name.".format(dup_cnt))

    # dedup on gene name mapping dictionary
    mapping = dataframe[['original']]

    mapping_filtered = mapping[~mapping.index.str.contains(r'^unmapped.*$')]

    logging.append("INFO: Mapped {} gene(s) to ensemble name.".format(mapping_filtered.shape[0]))

    unmapped_filtered = mapping[mapping.index.str.contains(r'^unmapped.*$')].sort_index(axis=0, ascending=False)
    unmapped_filtered['ensemble'] = unmapped_filtered.index

    if unmapped_filtered.shape[0] > 0:
        logging.append("INFO: Unable to map {} gene(s) to ensemble name.".format(unmapped_filtered.shape[0]))

    mapping_dedup_df = mapping_filtered[~mapping_filtered.index.duplicated()]

    # writes unmapped gene name along with return value from Redis database to a file
    write_to_file(unmapped_filtered, run_parameters['spreadsheet_name_full_path'], run_parameters['results_directory'],
                  "_UNMAPPED.tsv", use_index=False, use_header=False)

    # writes dedupped mapping between original gene name and ensemble name to a file
    write_to_file(mapping_dedup_df, run_parameters['spreadsheet_name_full_path'], run_parameters['results_directory'],
                  "_MAP.tsv", use_header=False)

    return output_df_mapped_dedup


def sanity_check_input_data(input_dataframe):
    """
    Checks the validity of user input spreadsheet data file, including duplication and nan

    Args:
        input_dataframe: user spreadsheet input file DataFrame, which is uploaded from frontend
        run_parameters: run_file parameter dictionary

    Returns:
        flag: Boolean value indicates the status of current check
        message: A message indicates the status of current check
    """
    logging.append("INFO: Start to run sanity checks for input data.")

    # Case 1: removes NA rows in index
    input_dataframe_idx_na_rmd = remove_na_index(input_dataframe)
    if input_dataframe_idx_na_rmd is None:
        return None

    # Case 2: checks the duplication on column name and removes it if exists
    input_dataframe_col_dedup = check_duplicate_column_name(input_dataframe_idx_na_rmd)
    if input_dataframe_col_dedup is None:
        return None

    # Case 3: checks the duplication on gene name and removes it if exists
    input_dataframe_genename_dedup = check_duplicate_row_name(input_dataframe_col_dedup)
    if input_dataframe_genename_dedup is None:
        return None

    logging.append("INFO: Finished running sanity check for input data.")

    return input_dataframe_genename_dedup


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
    logging.append("INFO: Start to pre-process phenotype data.")

    phenotype_df_genename_dedup = sanity_check_input_data(phenotype_df)
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
