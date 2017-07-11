import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln


class Testcheck_input_value_for_gsc_sc_common(unittest.TestCase):
    def setUp(self):
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

        self.input_phenotype_df = pd.DataFrame(
            [[1.1, 2.2, 3.3]],
            index=['drug1'],
            columns=['a', 'b', 'c']
        )

        self.input_phenotype_df_bad = pd.DataFrame(
            [[1.1, 2.2, 3.3]],
            index=['drug1'],
            columns=['d', 'e', 'f']
        )


    def tearDown(self):
        del self.input_df
        del self.input_phenotype_df
        del self.input_df_nan

    def test_check_input_value_for_gsc_sc_common(self):
        ret_df = data_cln.check_input_value_for_gsc_sc_common(self.input_df)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_nan_input_value_in_spreadsheet(self):
        ret_df = data_cln.check_input_value_for_gsc_sc_common(self.input_df_nan)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_check_text_input_value_in_spreadsheet(self):
        ret_df = data_cln.check_input_value_for_gsc_sc_common(self.input_df_text)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)


if __name__ == '__main__':
    unittest.main()

