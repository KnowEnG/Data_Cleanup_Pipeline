import logger

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

