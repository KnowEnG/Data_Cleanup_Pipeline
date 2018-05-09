import unittest
import pandas as pd
from utils.check_util import CheckUtil
import utils.log_util as logger


class Testcheck_phenotype_data(unittest.TestCase):
    def setUp(self):
        logger.init()

        self.input_phenotype_df = pd.DataFrame([[1, 0],
                                                [0, 0],
                                                [1, 1],
                                                [0, 1],
                                                [0, 0]],
                                               index=['a', "b", 'c', 'd', 'e'],
                                               columns=['a', 'b'])
        self.input_phenotype_df_bad_value = pd.DataFrame([[1, 0],
                                                          [3, 0],
                                                          [1, 1],
                                                          [0, 1],
                                                          [0, 0]],
                                                         index=['a', "b", 'c', 'd', 'e'],
                                                         columns=['a', 'b'])
        self.input_phenotype_df_nan = pd.DataFrame([[1, 0],
                                                    [0, None],
                                                    [0, 1],
                                                    [1, 0],
                                                    [0, 1],
                                                    [1, 1]],
                                                   index=['a', "b", 'c', 'd', 'e', 'f'],
                                                   columns=['a', 'b'])
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
        del self.input_phenotype_df
        del self.input_phenotype_df_bad_value
        del self.input_phenotype_df_nan
        del self.input_phenotype_df_negative
        del self.input_phenotype_df_pearson

    def test_check_phenotype_data(self):
        ret_phenotype = CheckUtil.check_phenotype_data(self.input_phenotype_df, 't_test')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_nan_spreadsheet_value(self):
        ret_phenotype = CheckUtil.check_phenotype_data(self.input_phenotype_df_nan, 't_test')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_text_spreadsheet_value(self):
        ret_phenotype = CheckUtil.check_phenotype_data(self.input_phenotype_df_pearson, 'pearson')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_negative_phenotype_value(self):
        ret_phenotype = CheckUtil.check_phenotype_data(self.input_phenotype_df_negative, 'pearson')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_phenotype_value_pearson(self):
        ret_phenotype = CheckUtil.check_phenotype_data(self.input_phenotype_df_negative, 'pearson')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(True, ret_phenotype_flag)

    def test_check_phenotype_value_t_test(self):
        ret_phenotype = CheckUtil.check_phenotype_data(self.input_phenotype_df_bad_value, 't_test')
        ret_phenotype_flag = ret_phenotype is not None
        self.assertEqual(False, ret_phenotype_flag)


if __name__ == '__main__':
    unittest.main()
