import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln

class TestCheck_value_set(unittest.TestCase):
    def setUp(self):
        self.input_df = pd.DataFrame([[1, 0],
                                          [0, 0],
                                          [1, 1]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])

        self.value_set_a = set([0,1])
        self.value_set_b = set([0])

    def tearDown(self):
        del self.input_df
        del self.value_set_a
        del self.value_set_b

    def test_check_value_set_pass(self):
        ret_flag, ret_msg = data_cln.check_value_set(self.input_df, self.value_set_a)
        self.assertEqual(True, ret_flag)

    def test_check_value_set_fail(self):
        ret_flag, ret_msg = data_cln.check_value_set(self.input_df, self.value_set_b)
        self.assertEqual(False, ret_flag)

if __name__ == '__main__':
    unittest.main()