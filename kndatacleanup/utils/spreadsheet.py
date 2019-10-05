import pandas
from . import log_util as logger
from .redis_util import RedisUtil


class SpreadSheet:
    def __init__(self):
        pass

    @staticmethod
    def check_user_spreadsheet_data(dataframe, check_na=False, dropna_colwise=False, check_real_number=False,
                                    check_positive_number=False):
        """
        Customized checks for input data (contains NA value, contains all real number, contains all positive number)
        Args:
            dataframe: input DataFrame to be checked
            check_na: check NA in DataFrame
            dropna_colwise: drop column which contains NA
            check_real_number: check only real number exists in DataFrame
            check_positive_number: check only positive number exists in DataFrame

        Returns:
            dataframe: cleaned DataFrame
        """
        # drop NA column wise in dataframe
        if dropna_colwise is True:
            # drops column which check NA in dataframe
            org_column_count = dataframe.shape[1]
            dataframe = dataframe.dropna(axis=1)
            diff_count = org_column_count - dataframe.shape[1]
            if diff_count > 0:
                logger.logging.append("INFO: Remove {} column(s) which contains NA.".format(diff_count))

            if dataframe.empty:
                logger.logging.append("ERROR: User spreadsheet is empty after removing NA column wise.")
                return None

        # checks if dataframe contains NA value
        if check_na is True:
            if dataframe.isnull().values.any():
                logger.logging.append("ERROR: This user spreadsheet contains NaN value.")
                return None

        # checks real number negative to positive infinite
        if check_real_number is True:
            if False in dataframe.applymap(lambda x: isinstance(x, (int, float))).values:
                logger.logging.append("ERROR: Found non-numeric value in user spreadsheet.")
                return None

        # checks if dataframe contains only non-negative number
        if check_positive_number is True:
            if False in dataframe.applymap(lambda x: x >= 0).values:
                logger.logging.append("ERROR: Found negative value in user spreadsheet.")
                return None

        return dataframe

    @staticmethod
    def remove_dataframe_indexer_duplication(input_dataframe):
        """
        Checks the validity of user input spreadsheet data file, including duplication and nan

        Args:
            input_dataframe: user spreadsheet input file DataFrame, which is uploaded from frontend
            run_parameters: run_file parameter dictionary

        Returns:
            flag: Boolean value indicates the status of current check
            message: A message indicates the status of current check
        """
        logger.logging.append("INFO: Start to run sanity checks for input data.")

        # Case 1: removes NA rows in index
        input_dataframe_idx_na_rmd = SpreadSheet.remove_na_index(input_dataframe)
        if input_dataframe_idx_na_rmd is None:
            return None

        # Case 2: checks the duplication on column name and removes it if exists
        input_dataframe_col_dedup = SpreadSheet.remove_duplicate_column_name(input_dataframe_idx_na_rmd)
        if input_dataframe_col_dedup is None:
            return None

        # Case 3: checks the duplication on gene name and removes it if exists
        input_dataframe_genename_dedup = SpreadSheet.remove_duplicate_row_name(input_dataframe_col_dedup)
        if input_dataframe_genename_dedup is None:
            return None

        logger.logging.append("INFO: Finished running sanity check for input data.")

        return input_dataframe_genename_dedup

    @staticmethod
    def map_ensemble_gene_name(dataframe, run_parameters):
        """
        Checks if the gene name follows ensemble format.

        Args:
            dataframe: input DataFrame
            run_parameters: user configuration from run_file

        Returns:
             output_df_mapped_dedup: cleaned DataFrame
        """

        redis_db = RedisUtil(run_parameters['redis_credential'],
                             run_parameters['source_hint'],
                             run_parameters['taxonid'])
        # copy index to new column named with 'user_supplied_gene_name'
        dataframe = dataframe.assign(user_supplied_gene_name=dataframe.index)
        redis_ret = redis_db.get_node_info(dataframe.index, "Gene")
        # extract ensemble names as a list from a call to redis database
        ensemble_names = [x[1] for x in redis_ret]

        # resets dataframe's index with ensembel name
        dataframe.index = pandas.Series(ensemble_names)
        # extracts all mapped rows in dataframe
        output_df_mapped = dataframe[~dataframe.index.str.contains(r'^unmapped.*$')]
        if output_df_mapped.empty:
            logger.logging.append("ERROR: No valid ensemble name can be found.")
            return None, None, None

        # removes the temporary added column to keep original shape
        output_df_mapped = output_df_mapped.drop('user_supplied_gene_name', axis=1)
        output_df_mapped_dedup = output_df_mapped[~output_df_mapped.index.duplicated()]

        dup_cnt = output_df_mapped.shape[0] - output_df_mapped_dedup.shape[0]
        if dup_cnt > 0:
            logger.logging.append("INFO: Found {} duplicate Ensembl gene name.".format(dup_cnt))

        # The following logic is to generate UNMAPPED/MAP file with two columns
        # extract two columns, index is ensembl name and column 'user_supplied_gene_name' is user supplied gene
        mapping = dataframe[['user_supplied_gene_name']]

        # filter the mapped gene
        map_filtered = mapping[~mapping.index.str.contains(r'^unmapped.*$')]
        logger.logging.append("INFO: Mapped {} gene(s) to ensemble name.".format(map_filtered.shape[0]))

        # filter the unmapped gene
        unmap_filtered = mapping[mapping.index.str.contains(r'^unmapped.*$')]
        if unmap_filtered.shape[0] > 0:
            logger.logging.append("INFO: Unable to map {} gene(s) to ensemble name.".format(unmap_filtered.shape[0]))

        # filter out the duplicated ensemble gene name
        map_filtered_dedup = map_filtered[~map_filtered.index.duplicated()]

        # adds a status column
        mapping = mapping.assign(status=dataframe.index)

        # filter the duplicate gene name and write them along with their corresponding user supplied gene name to a file
        mapping.loc[(~dataframe.index.str.contains(
            r'^unmapped.*$') & mapping.index.duplicated()), 'status'] = 'duplicate ensembl name'

        return output_df_mapped_dedup, map_filtered_dedup, mapping

    @staticmethod
    def impute_na(dataframe, option="reject"):
        """
        Impute NA value based on options user selected
        Args:
            dataframe: the dataframe to be imputed
            option:
                1. reject(default value): reject spreadsheet if we found NA
                2. remove: remove Nan row
                3. average: replace Nan value with row mean

        Returns:
            dataframe
        """
        if option == "reject":
            if dataframe.isnull().values.any():
                logger.logging.append("ERROR: User spreadsheet contains NaN value. Rejecting this spreadsheet.")
                return None
            logger.logging.append("INFO: There is no NA value in spreadsheet.")
            return dataframe
        elif option == "remove":
            if dataframe.isnull().values.any():
                dataframe_dropna = dataframe.dropna(axis=0)
                logger.logging.append(
                    "INFO: Remove {} row(s) containing NA value.".format(
                        dataframe.shape[0] - dataframe_dropna.shape[0]))
                return dataframe_dropna
            else:
                return dataframe
        elif option == 'average':
            if dataframe.isnull().values.any():
                dataframe_avg = dataframe.apply(lambda x: x.fillna(x.mean()), axis=0)
                logger.logging.append("INFO: Filled NA with mean value of its corresponding row.")
                return dataframe_avg
            else:
                return dataframe

        logger.logging.append("Warning: Found invalid option to operate on NA value. Skip imputing on NA value.")
        return dataframe

    @staticmethod
    def check_unique_values(dataframe, cnt=0):
        """
        Checks unique values count per column in the input dataframe no less than the cnt value
        Args:
            dataframe:
            cnt:

        Returns:

        """
        for column in dataframe:
            cur_col = dataframe[[column]].dropna(axis=0)

            if not cur_col.empty:
                count_values = cur_col[column].value_counts()
                if count_values[count_values < cnt].size < 0:
                    return False
        return True

    @staticmethod
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
            logger.logging.append("WARNING: Removed {} empty row(s).".format(diff))

        if dataframe_no_empty_line.empty:
            logger.logging.append(
                "ERROR: After removed {} empty row(s), original dataframe in shape ({},{}) "
                "becames empty.".format(diff, dataframe.shape[0], dataframe.shape[1]))
            return None

        return dataframe_no_empty_line

    @staticmethod
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
            logger.logging.append("WARNING: Removed {} row(s) which contains NA in index.".format(diff))

        if new_row_cnt == 0:
            logger.logging.append(
                "ERROR: After removed {} row(s) that contains NA in index, original dataframe "
                "in shape ({},{}) becames empty.".format(
                    diff, dataframe.shape[0], dataframe.shape[1]))
            return None

        return dataframe_rm_null_idx

    @staticmethod
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
            logger.logging.append("WARNING: Removed {} column(s) which contains NA in header.".format(diff))

        if diff > 0 and dataframe_rm_na_header.empty:
            logger.logging.append("ERROR: After removed {} column(s) that contains NA in header, original dataframe "
                                  "in shape ({},{}) becames empty.".format(diff, dataframe.shape[0],
                                                                           dataframe.shape[1]))
            return None

        return dataframe_rm_na_header

    @staticmethod
    def remove_duplicate_column_name(dataframe):
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
            logger.logging.append("ERROR: User spreadsheet becomes empty after remove column duplicates.")
            return None

        row_count_diff = len(dataframe_transpose.index) - len(dataframe_row_dedup.index)

        if row_count_diff > 0:
            logger.logging.append(
                "WARNING: Removed {} duplicate column(s) from user spreadsheet.".format(row_count_diff))
            return dataframe_row_dedup.T

        if row_count_diff == 0:
            logger.logging.append("INFO: No duplicate column name detected in this data set.")
            return dataframe_row_dedup.T

        if row_count_diff < 0:
            logger.logging.append("ERROR: An unexpected error occurred during checking duplicate column name.")
            return None

    @staticmethod
    def remove_duplicate_row_name(dataframe):
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
            logger.logging.append("ERROR: User spreadsheet becomes empty after remove column duplicates.")
            return None

        row_count_diff = len(dataframe.index) - len(dataframe_genename_dedup.index)
        if row_count_diff > 0:
            logger.logging.append("WARNING: Removed {} duplicate row(s) from user spreadsheet.".format(row_count_diff))
            return dataframe_genename_dedup

        if row_count_diff == 0:
            logger.logging.append("INFO: No duplicate row name detected in this data set.")
            return dataframe_genename_dedup

        if row_count_diff < 0:
            logger.logging.append("ERROR: An unexpected error occurred during checking duplicate row name.")
            return None
