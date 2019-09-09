import unittest

import numpy as np
import pandas as pd

from utils.check_util import CheckUtil
import utils.log_util as logger
from utils.transformation_util import TransformationUtil


class Testcheck_phenotype_data(unittest.TestCase):
    def setUp(self):
        logger.init()

    def tearDown(self):
        pass

    def test_check_ttest_and_edger(self):
        too_few_distinct_values_message = \
            TransformationUtil.too_few_distinct_values_message.substitute(\
                col='pheno1')
        too_few_samples_message = \
            TransformationUtil.too_few_samples_message.substitute(\
                col='pheno1', min_num_samples=2)
        converting_message = \
            TransformationUtil.converting_message.substitute(col='pheno1')
        expanding_message = \
            TransformationUtil.expanding_message.substitute(col='pheno1')
        test_dicts = [
            {
                'input': pd.DataFrame({'pheno1': []}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [np.nan]*4}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [-1]}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': ['one']}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [-1]*4}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': ['one']*4}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [-1, np.nan]*2}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': ['one', np.nan]*2}),
                'output': None,
                'log': [too_few_distinct_values_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*1 + [1]*1 + [np.nan]*0}),
                'output': None,
                'log': [too_few_samples_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [1.1]*1 + [2.1]*1 + [np.nan]*0}),
                'output': None,
                'log': [converting_message, \
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_2.1', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*1 + ['one']*1 + [np.nan]*0}),
                'output': None,
                'log': [converting_message, \
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_zero', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*1 + [1]*1 + [np.nan]*1}),
                'output': None,
                'log': [too_few_samples_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [1.1]*1 + [2.1]*1 + [np.nan]*1}),
                'output': None,
                'log': [converting_message, \
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_2.1', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*1 + ['one']*1 + [np.nan]*1}),
                'output': None,
                'log': [converting_message, \
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_zero', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*1 + [1]*2 + [np.nan]*2}),
                'output': None,
                'log': [too_few_samples_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [1.1]*1 + [2.1]*2 + [np.nan]*2}),
                'output': None,
                'log': [converting_message, \
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_2.1', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*1 + ['one']*2 + [np.nan]*2}),
                'output': None,
                'log': [converting_message, \
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_zero', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*2 + [1]*2 + [np.nan]*2}),
                'output': pd.DataFrame({'pheno1': [0]*2 + [1]*2 + [np.nan]*2}),
                'log': []
            },
            {
                'input': pd.DataFrame({'pheno1': [-1.1]*2 + [2.1]*2 + [np.nan]*2}),
                'output': pd.DataFrame({'pheno1_2.1': [0]*2 + [1]*2 + [np.nan]*2}),
                'log': [converting_message]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*2 + ['one']*2 + [np.nan]*2}),
                'output': pd.DataFrame({'pheno1_zero': [1]*2 + [0]*2 + [np.nan]*2}),
                'log': [converting_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*3 + [1]*3 + [np.nan]*2}),
                'output': pd.DataFrame({'pheno1': [0]*3 + [1]*3 + [np.nan]*2}),
                'log': []
            },
            {
                'input': pd.DataFrame({'pheno1': [-1.1]*3 + [2.1]*3 + [np.nan]*2}),
                'output': pd.DataFrame({'pheno1_2.1': [0]*3 + [1]*3 + [np.nan]*2}),
                'log': [converting_message]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*3 + ['one']*3 + [np.nan]*2}),
                'output': pd.DataFrame({'pheno1_zero': [1]*3 + [0]*3 + [np.nan]*2}),
                'log': [converting_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*1 + [1]*2 + [2]*2 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_1.0': [0]*1 + [1]*2 + [0]*2 + [np.nan]*2,
                    'pheno1_2.0': [0]*3 + [1]*2 + [np.nan]*2}),
                'log': [expanding_message,\
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_0.0', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': [-1.1]*1 + [2.1]*2 + [3.1]*2 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_2.1': [0]*1 + [1]*2 + [0]*2 + [np.nan]*2,
                    'pheno1_3.1': [0]*3 + [1]*2 + [np.nan]*2}),
                'log': [expanding_message,\
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_-1.1', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*1 + ['one']*2 + ['two']*2 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_one': [0]*1 + [1]*2 + [0]*2 + [np.nan]*2,
                    'pheno1_two': [0]*3 + [1]*2 + [np.nan]*2}),
                'log': [expanding_message,\
                    TransformationUtil.too_few_samples_message.substitute(\
                        col='pheno1_zero', min_num_samples=2)]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*2 + [1]*2 + [2]*2 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_0.0': [1]*2 + [0]*4 + [np.nan]*2,
                    'pheno1_1.0': [0]*2 + [1]*2 + [0]*2 + [np.nan]*2,
                    'pheno1_2.0': [0]*4 + [1]*2 + [np.nan]*2}),
                'log': [expanding_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [-1.1]*2 + [2.1]*2 + [3.1]*2 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_-1.1': [1]*2 + [0]*4 + [np.nan]*2,
                    'pheno1_2.1': [0]*2 + [1]*2 + [0]*2 + [np.nan]*2,
                    'pheno1_3.1': [0]*4 + [1]*2 + [np.nan]*2}),
                'log': [expanding_message]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*2 + ['one']*2 + ['two']*2 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_zero': [1]*2 + [0]*4 + [np.nan]*2,
                    'pheno1_one': [0]*2 + [1]*2 + [0]*2 + [np.nan]*2,
                    'pheno1_two': [0]*4 + [1]*2 + [np.nan]*2}),
                'log': [expanding_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [0]*3 + [1]*3 + [2]*3 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_0.0': [1]*3 + [0]*6 + [np.nan]*2,
                    'pheno1_1.0': [0]*3 + [1]*3 + [0]*3 + [np.nan]*2,
                    'pheno1_2.0': [0]*6 + [1]*3 + [np.nan]*2}),
                'log': [expanding_message]
            },
            {
                'input': pd.DataFrame({'pheno1': [-1.1]*3 + [2.1]*3 + [3.1]*3 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_-1.1': [1]*3 + [0]*6 + [np.nan]*2,
                    'pheno1_2.1': [0]*3 + [1]*3 + [0]*3 + [np.nan]*2,
                    'pheno1_3.1': [0]*6 + [1]*3 + [np.nan]*2}),
                'log': [expanding_message]
            },
            {
                'input': pd.DataFrame({'pheno1': ['zero']*3 + ['one']*3 + ['two']*3 + [np.nan]*2}),
                'output': pd.DataFrame({
                    'pheno1_zero': [1]*3 + [0]*6 + [np.nan]*2,
                    'pheno1_one': [0]*3 + [1]*3 + [0]*3 + [np.nan]*2,
                    'pheno1_two': [0]*6 + [1]*3 + [np.nan]*2}),
                'log': [expanding_message]
            }
        ]
        methods = ['t_test', 'edgeR']
        for test_dict in test_dicts:
            for method in methods:
                with self.subTest(test_dict=test_dict, method=method):
                    logger.init()
                    out_df = CheckUtil.check_phenotype_data(\
                        test_dict['input'], method)
                    if test_dict['output'] is None:
                        self.assertIsNone(out_df)
                    else:
                        self.assertTrue(test_dict['output'].equals(out_df), \
                            msg="Expected " + str(test_dict['output']) + " but got " + \
                            str(out_df) + ".")
                    self.assertEqual(logger.logging, test_dict['log'])

    def test_check_nan_spreadsheet_value(self):
        input_phenotype_df_nan = pd.DataFrame([[1, 0],
                                                    [0, None],
                                                    [0, 1],
                                                    [1, 0],
                                                    [0, 1],
                                                    [1, 1]],
                                                   index=['a', "b", 'c', 'd', 'e', 'f'],
                                                   columns=['a', 'b'])
        ret_phenotype = CheckUtil.check_phenotype_data(input_phenotype_df_nan, 't_test')
        self.assertIsNotNone(ret_phenotype)

    def test_check_text_spreadsheet_value(self):
        input_phenotype_df_pearson = pd.DataFrame(
            [[1.1, 0.1], [-2.2, 1.2], [3.3, 2.3]],
            index=['d', 'e', 'f'],
            columns=['drug1', 'drug2']
        )
        ret_phenotype = CheckUtil.check_phenotype_data(input_phenotype_df_pearson, 'pearson')
        self.assertIsNotNone(ret_phenotype)

    def test_check_negative_phenotype_value(self):
        input_phenotype_df_negative = pd.DataFrame(
            [[1.1], [-2.2], [3.3]],
            index=['a', 'b', 'f'],
            columns=['drug1']
        )
        ret_phenotype = CheckUtil.check_phenotype_data(input_phenotype_df_negative, 'pearson')
        self.assertIsNotNone(ret_phenotype)

    def test_check_phenotype_value_pearson(self):
        input_phenotype_df_negative = pd.DataFrame(
            [[1.1], [-2.2], [3.3]],
            index=['a', 'b', 'f'],
            columns=['drug1']
        )
        ret_phenotype = CheckUtil.check_phenotype_data(input_phenotype_df_negative, 'pearson')
        self.assertIsNotNone(ret_phenotype)

    def test_check_phenotype_value_t_test(self):
        input_phenotype_df_bad_value = pd.DataFrame([[1, 0],
                                                          [3, 0],
                                                          [1, 1],
                                                          [0, 1],
                                                          [0, 0]],
                                                         index=['a', "b", 'c', 'd', 'e'],
                                                         columns=['a', 'b'])

        ret_phenotype = CheckUtil.check_phenotype_data(input_phenotype_df_bad_value, 't_test')
        self.assertIsNotNone(ret_phenotype)


if __name__ == '__main__':
    unittest.main()
