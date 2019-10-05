import unittest
import pandas as pd
from kndatacleanup.utils.check_util import CheckUtil
import kndatacleanup.utils.log_util as logger

class TestCheck_user_spreadsheet_data(unittest.TestCase):
    def setUp(self):
        logger.init()

        self.input_df = pd.DataFrame([[1, 2],
                                      [0, 10],
                                      [1, 9]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])
        self.input_df_nan = pd.DataFrame([[1, 0],
                                          [0, None],
                                          [1, 1]],
                                         index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                         columns=['a', 'b'])
        self.input_df_text = pd.DataFrame([["text", 0],
                                           [0, "text"],
                                           [1, 1]],
                                          index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                          columns=['a', 'b'])
        self.input_df_negative = pd.DataFrame([[-1, 0],
                                               [0, -2],
                                               [1, 1]],
                                              index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                              columns=['a', 'b'])

    def tearDown(self):
        del self.input_df
        del self.input_df_nan

    def test_check_not_null_non_negative_real_number(self):
        ret_df = CheckUtil.check_user_spreadsheet_data(self.input_df, check_na=True, dropna_colwise=False,
                                                 check_real_number=True,
                                                 check_positive_number=True)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_nan_input_value_in_spreadsheet(self):
        ret_df = CheckUtil.check_user_spreadsheet_data(self.input_df_nan, check_na=True, dropna_colwise=False,
                                                 check_real_number=False,
                                                 check_positive_number=False)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_check_text_input_value_in_spreadsheet(self):
        ret_df = CheckUtil.check_user_spreadsheet_data(self.input_df_text, check_na=False, dropna_colwise=False,
                                                 check_real_number=True,
                                                 check_positive_number=False)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_check_negative_input_value(self):
        ret_df = CheckUtil.check_user_spreadsheet_data(self.input_df_negative, check_na=False, dropna_colwise=False,
                                                 check_real_number=False,
                                                 check_positive_number=True)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)


if __name__ == '__main__':
    unittest.main()
