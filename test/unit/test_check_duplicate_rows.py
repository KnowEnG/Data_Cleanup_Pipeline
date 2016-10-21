import unittest
import pandas as pd
import numpy.testing as npytest
import data_cleanup_toolbox as data_cln

class TestCheck_duplicate_rows(unittest.TestCase):
    def setUp(self):
        self.input_df_dup = pd.DataFrame([[1,2,3],[2,3,1],[1,2,3]])
        self.input_df_no_dup = pd.DataFrame([[1,2,3],[2,3,1],[4,2,3]])
        self.golden_output_dedup = pd.DataFrame([[1,2,3],[2,3,1]])
        self.golden_output_nodup = pd.DataFrame([[1,2,3],[2,3,1],[4,2,3]])

    def tearDown(self):
        del self.input_df_dup
        del self.input_df_no_dup

    def test_check_duplicate_row_with_dup(self):
        ret_df, flag, msg = data_cln.check_duplicate_rows(self.input_df_dup)
        self.assertEqual(True, flag)
        npytest.assert_array_equal(self.golden_output_dedup, ret_df)

    def test_check_duplicate_row_without_dup(self):
        ret_df, flag, msg = data_cln.check_duplicate_rows(self.input_df_no_dup)
        self.assertEqual(True, flag)
        npytest.assert_array_equal(self.golden_output_nodup, ret_df)


if __name__ == '__main__':
    unittest.main()
