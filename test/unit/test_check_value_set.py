import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln

class TestCheck_dataframe_value(unittest.TestCase):
    def setUp(self):
        self.input_df = pd.DataFrame([[1, 0],
                                          [0, 0],
                                          [1, 1]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])
        self.input_nan_df = pd.DataFrame([[1, 0],
                                          [0, None],
                                          [1, 1]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])
        self.value_set_a = set([0,1])
        self.value_set_b = set([0])
        self.pipeline_sc = "sc"
        self.spreadsheet_type = "sample"

    def tearDown(self):
        del self.input_df
        del self.value_set_a
        del self.value_set_b

    def test_check_dataframe_value_pass(self):
        ret_df, ret_msg = data_cln.check_dataframe_value(self.input_df, self.pipeline_sc, self.spreadsheet_type, self.value_set_a)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_dataframe_value_Nan_value(self):
        ret_df, ret_msg = data_cln.check_dataframe_value(self.input_nan_df, self.pipeline_sc, self.spreadsheet_type, self.value_set_a)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)


    def test_check_dataframe_value_sc_sample(self):
        ret_df, ret_msg = data_cln.check_dataframe_value(self.input_df, self.pipeline_sc, self.spreadsheet_type, self.value_set_a)


if __name__ == '__main__':
    unittest.main()