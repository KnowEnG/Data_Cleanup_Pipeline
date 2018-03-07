import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln
import numpy.testing as npytest


class TestImpute_na(unittest.TestCase):
    def setUp(self):
        self.input_df = pd.DataFrame([[1, 1, None],
                                      [2, 0, 0],
                                      [4, 1, 1]],
                                     index=['aa', "bb", 'cc'],
                                     columns=['a', 'b', 'c'])

        self.golden_output_remove = pd.DataFrame([[2, 0, 0],
                                      [4, 1, 1]],
                                     index=["bb", 'cc'],
                                     columns=['a', 'b', 'c'])

        self.golden_output_average = pd.DataFrame([[1, 1, 0.5],
                                      [2, 0, 0],
                                      [4, 1, 1]],
                                     index=['aa', "bb", 'cc'],
                                     columns=['a', 'b', 'c'])
    def tearDown(self):
        del self.input_df

    def test_impute_na_average(self):
        ret = data_cln.impute_na(self.input_df, "average")
        npytest.assert_array_equal(self.golden_output_average, ret)

    def test_impute_na_remove(self):
        ret = data_cln.impute_na(self.input_df, "remove")
        npytest.assert_array_equal(self.golden_output_remove, ret)
