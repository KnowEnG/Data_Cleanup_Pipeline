from string import Template

import pandas as pd
import numpy as np

import utils.log_util as logger

class TransformationUtil:

    too_few_distinct_values_message = Template("INFO: Dropping column $col " + \
        "because it doesn't have at least two distinct values.")

    too_few_samples_message = Template("INFO: Dropping column $col " + \
        "because it doesn't have at least $min_num_samples samples for 0 and for 1.")

    converting_message = Template("INFO: Converting binary column $col " + \
        "to 0/1 encoding.")

    expanding_message = Template("INFO: Expanding categoric column $col " + \
        "as indicator variables.")

    @staticmethod
    def encode_as_binary(input_df, min_num_samples):
        """
        Converts each column of the input dataframe to binary encoding. Intended
        for use with dataframes whose columns are binary or categorical, but it
        will not throw an exception if given numeric or free-text data; such columns
        will usually be excluded from the output due to the `min_num_samples`
        filtering.

        For each column, let `num_distinct_values` be the number of distinct values,
        excluding NA, in the column.
        - If `num_distinct_values` < 2, drop the column from the output.
        - If `num_distinct_values` == 2 and the two distinct values are 0 and 1,
            leave the column unchanged in the output.
        - If `num_distinct_values` == 2 and the two distinct values are not 0 and 1,
            convert the column for the output as follows: Replace all instances
            of one of the distinct values with 0 and replace all instances of the
            other distinct value with 1. Any missing values will remain unchanged.
            Edit the column name to indicate which of the original values is now
            represented by 1.
        - If `num_distinct_values` > 2, expand the column into `num_distinct_values`
            indicator columns in the output; any NAs will be preserved

        Finally, for each of the binary columns present at the end of the above
        procedure, count the number of samples having value 0 and the number of
        samples having value 1. If either of those counts is less than `min_num_samples`,
        drop the column from the output.

        Args:
            input_df (pandas.DataFrame): the dataframe to process.
            min_num_samples (int): the minimum number of samples that must have value 0
                and that must have value 1 in each of the output columns.

        Returns:
            pandas.DataFrame: a new dataframe derived from the input as described.
        """
        # create an empty df for output
        output_df = pd.DataFrame({}, index=input_df.index)

        # encode all columns as binary
        for col in input_df:

            s_col_values = input_df[col]

            # determine number of distinct values for this column
            s_value_counts = s_col_values.value_counts()

            # first ensure 0/1 encoding, including indicator variables for categorical case
            if len(s_value_counts) < 2:
                logger.logging.append(\
                    TransformationUtil.too_few_distinct_values_message.substitute(col=col))

            elif len(s_value_counts) == 2:
                if sorted(s_value_counts.index.values) == [0, 1]:
                    # column is already 0/1 encoded; add to output df
                    output_df = pd.concat([output_df, s_col_values], axis=1)
                else:
                    logger.logging.append(\
                        TransformationUtil.converting_message.substitute(col=col))
                    # convert to 0/1 encoding and then add to output df
                    output_col = pd.get_dummies(s_col_values, prefix=col, \
                        drop_first=True) # note drop_first=True to get 1 col back
                    # preserve NAs
                    output_col.loc[s_col_values.isnull(), :] = np.nan
                    output_df = pd.concat([output_df, output_col], axis=1)

            else:
                logger.logging.append(\
                    TransformationUtil.expanding_message.substitute(col=col))
                output_cols = pd.get_dummies(s_col_values, prefix=col)
                # preserve NAs
                output_cols.loc[s_col_values.isnull(), :] = np.nan
                output_df = pd.concat([output_df, output_cols], axis=1)

        # drop any columns without `min_num_samples` for 0 and 1
        for col in output_df:
            s_value_counts = output_df[col].value_counts()
            if any(s_value_counts < min_num_samples):
                logger.logging.append(\
                    TransformationUtil.too_few_samples_message.substitute(\
                        col=col, min_num_samples=min_num_samples))
                output_df.drop(col, axis=1, inplace=True)

        return output_df

    @staticmethod
    def force_string_columns_to_lowercase(in_out_df):
        """
        Given a dataframe, forces any columns of strings to be entirely lowercase.
        The change will be made *in place*; i.e., the input dataframe will be
        modified and returned as the output dataframe.

        Args:
            in_out_df: dataframe to process.

        Returns:
            in_out_df: the input dataframe, now modified.

        """
        for column in in_out_df:
            if in_out_df[column].dtype == object:
                in_out_df[column] = in_out_df[column].str.lower()
        return in_out_df
