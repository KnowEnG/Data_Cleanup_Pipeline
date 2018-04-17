import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln


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
            [[0], [1], [0]],
            index=['a', 'b', 'c'],
            columns=['drug1']
        )

        self.input_phenotype_df_negative = pd.DataFrame(
            [[1.1], [-2.2], [3.3]],
            index=['a', 'b', 'f'],
            columns=['drug1']
        )

        self.input_phenotype_df_pearson = pd.DataFrame(
            [[1.1, 0.1], [-2.2, 1.2], [3.3, 2.3]],
            index=['d', 'e', 'f'],
            columns=['drug1', 'drug2']
        )

    def tearDown(self):
        del self.input_df
        del self.input_df_text
        del self.input_df_nan
        del self.input_phenotype_df

    def test_check_phenotype_data(self):
        ret_phenotype = data_cln.check_phenotype_data(self.input_phenotype_df, 't_test')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_nan_spreadsheet_value(self):
        ret_phenotype = data_cln.check_phenotype_data(self.input_phenotype_df, 't_test')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_text_spreadsheet_value(self):
        ret_phenotype = data_cln.check_phenotype_data(self.input_phenotype_df, 'pearson')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_negative_phenotype_value(self):
        ret_phenotype = data_cln.check_phenotype_data(self.input_phenotype_df_negative, 'pearson')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_phenotype_value_pearson(self):
        ret_phenotype = data_cln.check_phenotype_data(self.input_phenotype_df_negative,
                                                                            'pearson')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_phenotype_value_t_test(self):
        ret_phenotype = data_cln.check_phenotype_data(self.input_phenotype_df,
                                                                            't_test')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)


if __name__ == '__main__':
    unittest.main()
