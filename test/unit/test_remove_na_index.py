import unittest
import pandas as pd
import utils.log_util as logger
from utils.transformation_util import TransformationUtil


class TestRemove_na_index(unittest.TestCase):
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
                                         index=['NA', "ENSG00001027003", 'ENSG00008000303'],
                                         columns=['a', 'b'])

        self.input_df_fail_none = pd.DataFrame([[1, 0],
                                           [0, 10],
                                           [1, 1]],
                                          index=[None, None, None],
                                          columns=['a', 'b'])

        self.input_df_fail_na = pd.DataFrame([[1, 0],
                                                [0, 10],
                                                [1, 1]],
                                               index=["na", None, None],
                                               columns=['a', 'b'])

    def tearDown(self):
        del self.input_df
        del self.input_df_nan
        del self.input_df_fail_none
        del self.input_df_fail_na

    def test_remove_na_index(self):
        ret_df = TransformationUtil.remove_na_index(self.input_df)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_remove_na_index_nan(self):
        ret_df = TransformationUtil.remove_na_index(self.input_df_nan)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_remove_na_index_fail_none(self):
        ret_df = TransformationUtil.remove_na_index(self.input_df_fail_none)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_remove_na_index_fail_na(self):
        ret_df = TransformationUtil.remove_na_index(self.input_df_fail_na)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)


if __name__ == '__main__':
    unittest.main()