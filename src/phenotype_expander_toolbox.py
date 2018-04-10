"""
@author: The KnowEnG dev team
"""
from enum import Enum
import pandas as pd
import numpy as np
import knpackage.toolbox as kn


class ColumnType(Enum):
    """Two categories of phenotype traits.
    """
    CONTINUOUS = "continuous"
    CATEGORICAL = "categorical"


def run_pre_processing_phenotype_expander(phenotype_df, threshold):
    """This is the preprocessing step to expand phenotype
    Parameters:
        phenotype_df: phenotype dataframe.
        threshold: threshold to determine which phenotype to remove.
    Returns:
        output_dict: dictionary with keys to be categories of phenotype data and values
        to be a list of related dataframes.
    """
    from collections import defaultdict

    output_dict = defaultdict(list)

    for column in phenotype_df:
        cur_df = phenotype_df[[column]].dropna(axis=0)

        if not cur_df.empty:
            if cur_df[column].dtype == object:
                cur_df_lowercase = cur_df.apply(lambda x: x.astype(str).str.lower())
            else:
                cur_df_lowercase = cur_df

            num_uniq_value = len(cur_df_lowercase[column].dropna().unique())

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


def phenotype_expander(run_parameters):
    """ Run phenotype expander on the whole dataframe of phenotype data.
    Save the results to tsv file.
    """
    phenotype_df = kn.get_spreadsheet_df(run_parameters['phenotype_name_full_path'])
    output_dict = run_pre_processing_phenotype_expander(phenotype_df, run_parameters['threshold'])

    result_df = pd.DataFrame(index=phenotype_df.index)

    for key, df_list in output_dict.items():
        if key == ColumnType.CATEGORICAL:
            for item in df_list:
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

