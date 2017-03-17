import unittest
import pandas as pd
import numpy.testing as npytest
import data_cleanup_toolbox as data_cln


class TestCheck_duplicate_gene_name(unittest.TestCase):
    def setUp(self):
        self.input_df_dup = pd.DataFrame([[1, 0],
                                          [0, 0],
                                          [1, 1]],
                                         index=['ENSG00000000003', "ENSG00001027003", 'ENSG00000000003'],
                                         columns=['a', 'b']
                                         )

        self.input_df_nodup = pd.DataFrame([[1, 0],
                                            [0, 0],
                                            [1, 1]],
                                           index=['ENSG00001027003', "ENSG00000002008", 'ENSG00008000303'],
                                           columns=['a', 'b']
                                           )

        self.golden_output_dedup = pd.DataFrame([[1, 0],
                                                 [0, 0]],
                                                index=[ "ENSG00000000003", 'ENSG00001027003'],
                                                columns=['a', 'b']
                                                )

        self.golden_output_nodup = pd.DataFrame([[1, 0],
                                                 [0, 0],
                                                 [1, 1]],
                                                index=['ENSG00001027003', "ENSG00000002008", 'ENSG00008000303'],
                                                columns=['a', 'b']
                                                )

    def tearDown(self):
        del self.input_df_dup
        del self.input_df_nodup
        del self.golden_output_dedup
        del self.golden_output_nodup

    def test_check_duplicate_row_name_with_dup(self):
        ret_df = data_cln.check_duplicate_row_name(self.input_df_dup)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)
        npytest.assert_array_equal(self.golden_output_dedup, ret_df)

    def test_check_duplicate_row_name_without_dup(self):
        ret_df = data_cln.check_duplicate_row_name(self.input_df_nodup)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)
        npytest.assert_array_equal(self.golden_output_nodup, ret_df)


if __name__ == '__main__':
    unittest.main()
