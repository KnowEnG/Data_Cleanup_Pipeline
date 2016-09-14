import unittest
import pandas as pd
import numpy.testing as npytest
import input_data_cleaning as data_cln

class TestCheck_duplicate_gene_name(unittest.TestCase):
    def setUp(self):
        self.input_df_dup = pd.DataFrame([["ENSG00001027003", 1, 0],
                                          ["ENSG00001027003", 0, 0],
                                          ["ENSG00008000303", 1, 1]])
        
        self.input_df_nodup = pd.DataFrame([["ENSG00001027003", 1, 0],
                                          ["ENSG00000002008", 0, 0],
                                          ["ENSG00008000303", 1, 1]])
        
        self.golden_output_dedup = pd.DataFrame([["ENSG00001027003", 1, 0],
                                          ["ENSG00008000303", 1, 1]])
        
        self.golden_output_nodup = pd.DataFrame([["ENSG00001027003", 1, 0],
                                          ["ENSG00000002008", 0, 0],
                                          ["ENSG00008000303", 1, 1]])

    def tearDown(self):
        del self.input_df_dup
        del self.input_df_nodup
        del self.golden_output_dedup
        del self.golden_output_nodup

    def test_check_duplicate_gene_name_with_dup(self):
        ret_df, ret_flag, ret_msg = data_cln.check_duplicate_gene_name(self.input_df_dup)
        self.assertEqual(False, ret_flag)
        npytest.assert_array_equal(self.golden_output_dedup, ret_df)
        
    def test_check_duplicate_gene_name_without_dup(self):
        ret_df, ret_flag, ret_msg = data_cln.check_duplicate_gene_name(self.input_df_nodup)
        self.assertEqual(True, ret_flag)
        npytest.assert_array_equal(self.golden_output_nodup, ret_df)

if __name__ == '__main__':
    unittest.main()