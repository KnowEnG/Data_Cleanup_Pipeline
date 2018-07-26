import unittest
import pandas as pd
import numpy.testing as npytest
from utils.spreadsheet import SpreadSheet
import utils.log_util as logger


class TestImpute_na(unittest.TestCase):
    def setUp(self):
        logger.init()

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
        ret = SpreadSheet.impute_na(self.input_df, "average")
        npytest.assert_array_equal(self.golden_output_average, ret)

    def test_impute_na_remove(self):
        ret = SpreadSheet.impute_na(self.input_df, "remove")
        npytest.assert_array_equal(self.golden_output_remove, ret)

    def test_impute_na_bad_option(self):
        ret = SpreadSheet.impute_na(self.input_df, "bad")
        npytest.assert_array_equal(self.input_df, ret)



if __name__ == '__main__':
    logger.init()
    unittest.main()
