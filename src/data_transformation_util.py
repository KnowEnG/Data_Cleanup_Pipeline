import logger
import pandas as pd
import numpy as np


class DataTransformationUtil:
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
                           "in shape ({},{}) becames empty.".format(diff, dataframe.shape[0], dataframe.shape[1]))
            return None

        return dataframe_rm_na_header

    @staticmethod
    def impute_na(dataframe, option="reject"):
        '''
        Impute NA value based on options user selected
        Args:
            dataframe: the dataframe to be imputed
            option:
                1. reject(default value): reject spreadsheet if we found NA
                2. remove: remove Nan row
                3. average: replace Nan value with row mean

        Returns:
            dataframe
        '''
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
    def phenotype_expander(phenotype_df):
        """
        Expands phenotype on t-test mode with following three conditions:
            case 1: phenotype data is binary data set
            case 2: phenotype data has two unique value/categories(excluding NaN)
            case 3: phenotype data has more than two unique value/categories
        Args:
            phenotype_df: input original phenotype dataframe

        Returns:
            expanded phenotype dataframe
        """
        # Standardizes data to be all lower case if there is any data with String type and process binary column
        output_list = DataTransformationUtil.uniform_phenotype_data(phenotype_df)

        # Creates an empty result_df
        result_df = pd.DataFrame(index=phenotype_df.index)

        for item in output_list:
            col_df = phenotype_df.loc[:, item.columns[0]].dropna()
            uniq_array = np.unique(col_df.values)

            # Expands only on columns which have more than two categories
            if uniq_array.shape[0] > 2:
                col_names = [item.columns[0] + '_' + str(i) for i in uniq_array]
                cur_df = pd.DataFrame(columns=col_names, index=col_df.index)
                cur_append_df = pd.DataFrame(columns=col_names, index=phenotype_df.index)
                for i, val in enumerate(uniq_array):
                    cur_df.loc[col_df == val, col_names[i]] = 1
                    cur_df.loc[col_df != val, col_names[i]] = 0
                cur_append_df.loc[cur_df.index, :] = cur_df
                result_df = pd.concat([result_df, cur_append_df], axis=1)
            else:
                result_df[item.columns[0]] = item

        result_df.index.name = "sample_id"
        return result_df

    @staticmethod
    def uniform_phenotype_data(phenotype_df):
        """
        This function will standardize phenotype data. If there is any string object, make them all as lower case.
        Args:
            phenotype_df: phenotype dataframe

        Returns:
            output_list: list with all column with standardized value.
        """

        output_list = []
        # Binary value set
        binary_value_set = {0, 1}

        for column in phenotype_df:
            cur_df_wona = phenotype_df[[column]].dropna(axis=0)
            org_df = phenotype_df[[column]]

            if not cur_df_wona.empty:
                if cur_df_wona[column].dtype == object:
                    cur_df_wona = cur_df_wona.apply(lambda x: x.astype(str).str.lower())

                # Checks if original phenotype data is binary data with value {0,1}. If so, return phenotype
                list_values = pd.unique(cur_df_wona.values.ravel())

                # Removes Nan value from list and gets the unique value set
                cur_value_set = set(filter(lambda x: x == x, list_values))

                # Case 1: phenotype data has more than two unique values
                if len(cur_value_set) > 2:
                    output_list.append(cur_df_wona)
                else:
                    # Case 2: phenotype data is binary data set. Note: True/False will also fit into this condition
                    if cur_value_set == binary_value_set:
                        org_df = org_df.fillna(2)
                        org_df.apply(np.int64)
                        org_df = org_df.replace(2, np.nan)
                    # Case 3: phenotype data has two unqiue value/categories(excluding NaN), but not binary, ex:{a,b}
                    elif len(cur_value_set) == 2:
                        phenotype_value_list = list(cur_value_set)
                        phenotype_value_list.sort()
                        org_df = org_df.replace(phenotype_value_list[0], 1)
                        org_df = org_df.replace(phenotype_value_list[1], 0)

                    output_list.append(org_df)

        return output_list
