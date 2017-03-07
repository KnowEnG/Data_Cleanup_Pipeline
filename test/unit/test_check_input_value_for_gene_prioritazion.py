import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln
import os


class Testcheck_input_value(unittest.TestCase):
    def setUp(self):
        self.input_df = pd.DataFrame([[1, 0],
                                      [0, 0],
                                      [1, 1]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])
        self.input_df_text = pd.DataFrame([[1, 0],
                                           [0, "text"],
                                           [1, 1]],
                                          index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                          columns=['a', 'b'])
        self.input_df_nan = pd.DataFrame([[1, 0],
                                          [0, None],
                                          [1, 1]],
                                         index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                         columns=['a', 'b'])


        self.input_phenotype_df = pd.DataFrame(
            [[0, 1, 0]],
            index=['drug1'],
            columns=['a', 'b', 'c']
        )

        self.input_phenotype_df_negative = pd.DataFrame(
            [[1.1, -2.2, 3.3]],
            index=['drug1'],
            columns=['d', 'e', 'f']
        )

        self.input_phenotype_df_pearson = pd.DataFrame(
            [[1.1, -2.2, 3.3], [0.1, -1.2, 2.3]],
            index=['drug1', 'drug2'],
            columns=['d', 'e', 'f']
        )

    def tearDown(self):
        del self.input_df
        del self.input_df_text
        del self.input_df_nan
        del self.input_phenotype_df

    def test_check_input_value_for_gene_prioritization(self):
        ret_df, ret_phenotype, ret_msg = data_cln.check_input_value_for_gene_prioritization(self.input_df,
                                                                                          self.input_phenotype_df, 't_test')
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_nan_spreadsheet_value(self):
        ret_df, ret_phenotype, ret_msg = data_cln.check_input_value_for_gene_prioritization(self.input_df_nan,
                                                                                          self.input_phenotype_df, 't_test')
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_text_spreadsheet_value(self):
        ret_df, ret_phenotype, ret_msg = data_cln.check_input_value_for_gene_prioritization(self.input_df_text,
                                                                                          self.input_phenotype_df, 'pearson')
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_check_negative_phenotype_value(self):
        ret_df, ret_phenotype, ret_msg = data_cln.check_input_value_for_gene_prioritization(self.input_df,
                                                                                          self.input_phenotype_df_negative, 'pearson')
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_phenotype_value_pearson(self):
        ret_df, ret_phenotype, ret_msg = data_cln.check_input_value_for_gene_prioritization(self.input_df,
                                                                                            self.input_phenotype_df_negative,
                                                                                            'pearson')
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_phenotype_value_t_test(self):
        ret_df, ret_phenotype, ret_msg = data_cln.check_input_value_for_gene_prioritization(self.input_df,
                                                                                            self.input_phenotype_df,
                                                                                            't_test')
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)


if __name__ == '__main__':
    unittest.main()
