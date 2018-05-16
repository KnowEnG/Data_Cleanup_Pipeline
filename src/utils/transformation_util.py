import pandas as pd
import numpy as np


class TransformationUtil:
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
        output_list = TransformationUtil.uniform_phenotype_data(phenotype_df)

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
