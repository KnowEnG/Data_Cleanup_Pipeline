import unittest
import pandas as pd
from kndatacleanup.utils.common_util import CommonUtil
import kndatacleanup.utils.log_util as logger


class TestRemove_dataframe_indexer_duplication(unittest.TestCase):
    def setUp(self):
        logger.init()
        self.input_df_nonnegative = pd.DataFrame(
            [[1, 0],
             [0, 0],
             [1, 1]],
            index=['ENSG00000000003', "ENSG00000000457", 'ENSG00000000005'],
            columns=['a', 'b']
        )
        self.input_df_negative = pd.DataFrame(
            [[1, 0],
             [0, -2],
             [1, 1]],
            index=['ENSG00000000003', "ENSG00000000457", 'ENSG00000000005'],
            columns=['a', 'b']
        )
        self.input_phenotype = pd.DataFrame(
            [[1],
             [0],
             [1],
             [0]],
            index=['a', 'b', 'c', 'd'],
            columns=['drug1']
        )

    def tearDown(self):
        del self.input_df_nonnegative
        del self.input_df_negative
        del self.input_phenotype

    def test_Accept_nonnegative_for_edgeR(self):
        ret1, ret2 = CommonUtil.validate_inputs_for_gp_fp(\
            self.input_df_nonnegative, self.input_phenotype, 'edgeR')
        self.assertIsNotNone(ret1)
        self.assertIsNotNone(ret2)

    def test_Reject_negative_for_edgeR(self):
        ret1, ret2 = CommonUtil.validate_inputs_for_gp_fp(\
            self.input_df_negative, self.input_phenotype, 'edgeR')
        self.assertIsNone(ret1)
        self.assertIsNone(ret2)

    def test_Accept_nonnegative_for_t_test(self):
        ret1, ret2 = CommonUtil.validate_inputs_for_gp_fp(\
            self.input_df_nonnegative, self.input_phenotype, 't_test')
        self.assertIsNotNone(ret1)
        self.assertIsNotNone(ret2)

    def test_Accept_negative_for_t_test(self):
        ret1, ret2 = CommonUtil.validate_inputs_for_gp_fp(\
            self.input_df_negative, self.input_phenotype, 't_test')
        self.assertIsNotNone(ret1)
        self.assertIsNotNone(ret2)

    def test_Accept_nonnegative_for_pearson(self):
        ret1, ret2 = CommonUtil.validate_inputs_for_gp_fp(\
            self.input_df_nonnegative, self.input_phenotype, 'pearson')
        self.assertIsNotNone(ret1)
        self.assertIsNotNone(ret2)

    def test_Accept_negative_for_pearson(self):
        ret1, ret2 = CommonUtil.validate_inputs_for_gp_fp(\
            self.input_df_negative, self.input_phenotype, 'pearson')
        self.assertIsNotNone(ret1)
        self.assertIsNotNone(ret2)

if __name__ == '__main__':
    unittest.main()
