"""
@author: The KnowEnG dev team
"""
import pandas as pd
import numpy as np


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
    # Checks if original phenotype data is binary data with value {0,1}. If so, return phenotype
    binary_data_set = {0, 1}
    list_values = pd.unique(phenotype_df.values.ravel())

    # Removes Nan value from list and gets the unique value set
    phenotype_value_set = set(filter(lambda x: x == x, list_values))

    # case 1: phenotype data is binary data set
    if binary_data_set == phenotype_value_set:
        return phenotype_df.apply(np.int64)
    elif len(phenotype_value_set) == 2:
        # case 2: phenotype data has two unqiue value/categories(excluding NaN)
        i = 0
        for item in phenotype_value_set:
            phenotype_df.replace(to_replace=item, value=i, inplace=True)
            i += 1
        return phenotype_df
    else:
        # case 3: phenotype data has more than two unique value/categories
        # standardizes data to be all lower case if there is any data with String type
        output_list = uniform_phenotype_data(phenotype_df)
        result_df = pd.DataFrame(index=phenotype_df.index)

        for item in output_list:
            col_df = phenotype_df.loc[:, item.columns[0]].dropna()
            uniq_array = np.unique(col_df.values)
            col_names = [item.columns[0] + '_' + str(i) for i in uniq_array]
            cur_df = pd.DataFrame(columns=col_names, index=col_df.index)
            cur_append_df = pd.DataFrame(columns=col_names, index=phenotype_df.index)

            for i, val in enumerate(uniq_array):
                cur_df.loc[col_df == val, col_names[i]] = 1
                cur_df.loc[col_df != val, col_names[i]] = 0
            cur_append_df.loc[cur_df.index, :] = cur_df
            result_df = pd.concat([result_df, cur_append_df], axis=1)

        result_df.index.name = "sample_id"

        return result_df


def uniform_phenotype_data(phenotype_df):
    """
    This function will standardize phenotype data. If there is any string object, make them all as lower case.
    Args:
        phenotype_df: phenotype dataframe

    Returns:
        output_list: list with all column with standardized value.
    """

    output_list = []

    for column in phenotype_df:
        cur_df = phenotype_df[[column]].dropna(axis=0)

        if not cur_df.empty:
            if cur_df[column].dtype == object:
                cur_df_lowercase = cur_df.apply(lambda x: x.astype(str).str.lower())
            else:
                cur_df_lowercase = cur_df

            output_list.append(cur_df_lowercase)

    return output_list

