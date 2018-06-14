import unittest
import pandas as pd
import utils.log_util as logger
from utils.spreadsheet import SpreadSheet


class TestRemove_na_header(unittest.TestCase):
    def setUp(self):
        logger.init()

        self.input_df = pd.DataFrame([[1, 2],
                                      [0, 10],
                                      [1, 9]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])

        self.input_df_nan = pd.DataFrame([[1, 0],
                                      [0, 10],
                                      [1, 1]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', None])

        self.input_df_fail = pd.DataFrame([[1, 0],
                                      [0, 10],
                                      [1, 1]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=[None, None])

    def tearDown(self):
        del self.input_df
        del self.input_df_nan
        del self.input_df_fail

    def test_remove_na_header(self):
        ret_df = SpreadSheet.remove_na_header(self.input_df)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_remove_na_header_nan_header(self):
        ret_df = SpreadSheet.remove_na_header(self.input_df_nan)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_remove_na_header_failure(self):
        ret_df = SpreadSheet.remove_na_header(self.input_df_fail)
        ret_flag = ret_df is not None

        self.assertEqual(False, ret_flag)


if __name__ == '__main__':
    unittest.main()
