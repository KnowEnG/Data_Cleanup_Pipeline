"""
    This module serves as a connecting function between front end and back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. 
"""
import pandas
import knpackage.redis_utilities as redisutil
import os

log_warnings = []

def run_geneset_characterization_pipeline(run_parameters):
    """
    Runs data cleaning for geneset_characterization_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df, ret_msg = load_data_file(run_parameters['spreadsheet_name_full_path'])
    
    if user_spreadsheet_df is None:
        return False, ret_msg

    # Value check logic a: checks if only 0 and 1 appears in user spreadsheet and rename phenotype data file to have _ETL.tsv suffix
    user_spreadsheet_val_chked, ret_msg = check_input_value_for_geneset_characterization(user_spreadsheet_df)

    if user_spreadsheet_val_chked is None:
        return False, ret_msg

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned, ret_msg = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if user_spreadsheet_df_cleaned is None:
        return False, ret_msg
    else:
        user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                sep='\t', header=True, index=True)
        log_warnings.append("WARNING: Cleaned user_spreadsheet has {} rows, {} columns.".format(user_spreadsheet_df_cleaned.shape[0],
                                                                                   user_spreadsheet_df_cleaned.shape[1]))
    return True, log_warnings


def run_samples_clustering_pipeline(run_parameters):
    """
    Runs data cleaning for samples_clustering_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df, ret_msg = load_data_file(run_parameters['spreadsheet_name_full_path'])

    if user_spreadsheet_df is None:
        return False, ret_msg

    # Value check logic a: checks if only real number appears in user spreadsheet and create absolute value
    user_spreadsheet_val_chked, ret_msg = check_input_value_for_samples_clustering(user_spreadsheet_df)

    if user_spreadsheet_val_chked is None:
        return False, ret_msg

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned, ret_msg = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if user_spreadsheet_df_cleaned is None:
        return False, ret_msg
    else:
        user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                sep='\t', header=True, index=True)
        log_warnings.append(
            "INFO: Cleaned user_spreadsheet has {} rows, {} columns.".format(user_spreadsheet_df_cleaned.shape[0],
                                                                               user_spreadsheet_df_cleaned.shape[1]))

    return True, log_warnings


def run_gene_prioritization_pipeline(run_parameters):
    """
    Runs data cleaning for gene_prioritization_pipeline.

    Args:
        run_parameters: configuration dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    user_spreadsheet_df, ret_msg = load_data_file(run_parameters['spreadsheet_name_full_path'])
    if user_spreadsheet_df is None:
        return False, ret_msg

    phenotype_df, ret_msg = load_data_file(run_parameters['phenotype_full_path'])
    if phenotype_df is None:
        return False, ret_msg

    # Value check logic b: checks if only 0 and 1 appears in user spreadsheet or if satisfies certain criteria
    user_spreadsheet_val_chked, phenotype_val_checked, ret_msg = check_input_value_for_gene_prioritization(
        user_spreadsheet_df, phenotype_df, run_parameters['correlation_measure'])

    if user_spreadsheet_val_chked is None:
        return False, ret_msg

    # Other checks including duplicate column/row name check and gene name to ensemble name mapping check
    user_spreadsheet_df_cleaned, ret_msg = sanity_check_user_spreadsheet(user_spreadsheet_val_chked, run_parameters)

    if user_spreadsheet_df_cleaned is None or phenotype_val_checked is None:
        return False, ret_msg
    else:
        # store cleaned phenotype data to a file
        phenotype_val_checked.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['phenotype_full_path']) + "_ETL.tsv",
                                     sep='\t', header=True, index=True)
        user_spreadsheet_df_cleaned.to_csv(run_parameters['results_directory'] + '/' + get_file_basename(
            run_parameters['spreadsheet_name_full_path']) + "_ETL.tsv",
                                     sep='\t', header=True, index=True)
        log_warnings.append(
            "WARNING: Cleaned user_spreadsheet has {} rows, {} columns.".format(user_spreadsheet_df_cleaned.shape[0],
                                                                               user_spreadsheet_df_cleaned.shape[1]))
    return True, log_warnings


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
        log_warnings.append("WARNING: Removed {} rows which contain NA in index.".format(diff))
    if dataframe_rm_na_idx.empty:
        return None, "After removed {} rows that contains NA in index, the dataframe becames empty.".format(diff)
    return dataframe_rm_na_idx, "Successfully removed {} rows which contains NA in index.".format(diff)


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


def load_data_file(spreadsheet_path):
    """
    Loads data file as a data frame object by a given file path

    Args:
        spreadsheet_path: user spreadsheet input file, which is uploaded from frontend

    Returns:
        user_spreadsheet_df: user spreadsheet as a data frame
    """
    if spreadsheet_path == None:
        return None, "Input file path is empty. Please provide a valid input path."

    try:
        user_spreadsheet_df = pandas.read_csv(spreadsheet_path, sep='\t', index_col=0, header=0, mangle_dupe_cols=False)
        if user_spreadsheet_df.empty:
            return None, "Input data is empty. Please provide a valid input data."
        return user_spreadsheet_df, "Successfully loaded input data."
    except OSError as err:
        return None, str(err)


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
    org_column_cnt = data_frame.shape[1]
    data_frame_transpose = data_frame.T
    user_spreadsheet_df_genename_dedup, ret_msg = check_duplicate_row_name(data_frame_transpose)

    if user_spreadsheet_df_genename_dedup is None:
        return False, ret_msg

    new_column_cnt = user_spreadsheet_df_genename_dedup.shape[0]
    diff = org_column_cnt - new_column_cnt
    if(diff > 0):
        log_warnings.append("WARNING: Removed {} duplicate columns from user spreadsheet.".format(diff))
    return user_spreadsheet_df_genename_dedup.T, ret_msg


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
    row_count_diff = len(data_frame.index) - len(data_frame_genename_dedup.index)

    if row_count_diff > 0:
        return data_frame_genename_dedup, "Found duplicate gene names " \
                                          "and dropped these duplicates. "

    if row_count_diff == 0:
        return data_frame_genename_dedup, "No duplication detected in this data set."

    return None, "An unexpected error occurred during checking duplicate row name."


def check_na_index_header(df_series):

    check_null_series = pandas.isnull(df_series)
    if True in check_null_series:
        return False, "Found NA in input series."

    return

def check_input_value_for_gene_prioritization(data_frame, phenotype_df, correlation_measure):
    
    # drops column which contains NA in data_frame
    data_frame_dropna = data_frame.dropna(axis=1)

    if data_frame_dropna.empty:
        return None, None, "User spreadsheet is empty after removing NA."

    # checks real number negative to positive infinite
    data_frame_check = data_frame_dropna.applymap(lambda x: isinstance(x, (int, float)))

    if False in data_frame_check.values:
        return None, None, "Found non-numeric value in user spreadsheet."

    # defines the default values that can exist in phenotype data
    gold_value_set = {0, 1}

    if correlation_measure == 't_test':
        phenotype_value_set = set(phenotype_df.ix[:, phenotype_df.columns != 0].values.ravel())
        if gold_value_set != phenotype_value_set:
            return None, None, "Only 0, 1 are allowed in phenotype data. This phenotype data contains invalid value: {}. ".format(
                phenotype_value_set) + "Please revise your phenotype and reupload."

    if correlation_measure == 'pearson':
        phenotype_df_check = phenotype_df.applymap(lambda x: isinstance(x, (int, float)))
        if False in phenotype_df_check:
            return None, None, "Found non-numeric value in phenotype data."

    return data_frame_dropna, phenotype_df, "Value contains in both user spreadsheet and phenotype data matches with gold standard value set."



def check_input_value_for_geneset_characterization(data_frame):
    """
    Checks if the values in user spreadsheet matches with gold standard value set
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
        return None, "This user spreadsheet contains invalid NaN value."

    gene_value_set = set(data_frame.ix[:, data_frame.columns != 0].values.ravel())

    if gold_value_set != gene_value_set:
        return None, "Only 0, 1 are allowed in user spreadsheet. This user spreadsheet contains invalid value: {}. ".format(
            gene_value_set) + \
               "Please revise your spreadsheet and reupload."

    return data_frame, "Value contains in user spreadsheet matches with gold standard value set."


def check_input_value_for_samples_clustering(data_frame):
    """
    Checks if the values in user spreadsheet matches with gold standard value set
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
        return None, "This user spreadsheet contains invalid NaN value."

    # checks if it contains only real number
    data_frame_real_number = data_frame.applymap(lambda x: isinstance(x, (int, float)))

    if False in data_frame_real_number.values:
        return None, "Found non-numeric value in user spreadsheet."

    # checks number of negative values
    data_frame_negative_cnt = data_frame.lt(0).sum().sum()
    log_warnings.append("WARNING: Converted {} negative number to their positive value.".format(data_frame_negative_cnt))

    # checks if it contains only positive number
    data_frame_abs = data_frame.abs()

    return data_frame_abs, "Value contains in user spreadsheet matches with gold standard value set."


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

    unmapped_filtered = mapping[mapping.index.str.contains(r'^unmapped.*$')].sort_index(axis=0, ascending=False)
    unmapped_filtered['ensemble'] = unmapped_filtered.index

    mapping_dedup_df = mapping_filtered[~mapping_filtered.index.duplicated()]

    output_file_basename = get_file_basename(run_parameters['spreadsheet_name_full_path'])

    # writes unmapped gene name along with return value from Redis database to a file
    unmapped_filtered.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_UNMAPPED.tsv",
                             sep='\t', header=False, index=False)

    # writes dedupped mapping between original gene name and ensemble name to a file
    mapping_dedup_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_MAP.tsv",
                            sep='\t', header=False, index=True)

    if output_df_mapped.empty:
        return None, "No valid ensemble name can be found."

    return output_df_mapped, "This is a valid user spreadsheet. Proceed to next step analysis."


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
    # Case 1: remove NA rows in index
    user_spreadsheet_df_idx_na_rmd, ret_msg = remove_na_index(user_spreadsheet_df)
    if user_spreadsheet_df_idx_na_rmd is None:
        return None, ret_msg
    
    # Case 2: checks the duplication on column name and removes it if exists
    user_spreadsheet_df_col_dedup, ret_msg = check_duplicate_column_name(user_spreadsheet_df_idx_na_rmd)

    if user_spreadsheet_df_col_dedup is None:
        return None, ret_msg

    # Case 3: checks the duplication on gene name and removes it if exists
    user_spreadsheet_df_genename_dedup, ret_msg = check_duplicate_row_name(user_spreadsheet_df_col_dedup)

    if user_spreadsheet_df_genename_dedup is None:
        return None, ret_msg

    # Case 4: checks the validity of gene name meaning if it can be ensemble or not
    user_spreadsheet_df_final, ret_msg = check_ensemble_gene_name(user_spreadsheet_df_genename_dedup, run_parameters)

    return user_spreadsheet_df_final, ret_msg


from enum import Enum
class ColumnType(Enum):
    """Two categories of phenotype traits.
    """
    CONTINUOUS = "continuous"
    CATEGORICAL = "categorical"


def run_post_processing_phenotype_clustering_data(cluster_phenotype_df, threshold):
    """This is the clean up function of phenotype data with nans removed.

    Parameters:
        cluster_phenotype_df: phenotype dataframe with the first column as sample clusters.
        threshold: threshold to determine which phenotype to remove.
    Returns:
        output_dict: dictionary with keys to be categories of phenotype data and values
        to be a list of related dataframes.
    """
    from collections import defaultdict

    output_dict = defaultdict(list)

    for column in cluster_phenotype_df:
        if column == 'Cluster_ID':
            continue
        cur_df = cluster_phenotype_df[['Cluster_ID', column]].dropna(axis=0)

        if not cur_df.empty:
            if cur_df[column].dtype == object:
                cur_df_lowercase = cur_df.apply(lambda x: x.astype(str).str.lower())
            else:
                cur_df_lowercase = cur_df
            num_uniq_value = len(cur_df_lowercase[column].unique())
            if num_uniq_value == 1:
                continue
            if cur_df_lowercase[column].dtype == object and num_uniq_value > threshold:
                continue
            if num_uniq_value > threshold:
                classification = ColumnType.CONTINUOUS
            else:
                classification = ColumnType.CATEGORICAL
            output_dict[classification].append(cur_df_lowercase)
    return output_dict


def generate_logging(flag, message, path):
    import yaml
    if flag:
        status = "SUCCESS"
    else:
        status = "FAIL"
    file_content = {status : message}
    output_stream = open(path, "w")
    yaml.dump(file_content, output_stream, default_flow_style=False)
    output_stream.close()