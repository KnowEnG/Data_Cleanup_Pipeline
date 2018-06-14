import unittest
import pandas as pd
import numpy.testing as npytest
from utils.spreadsheet import SpreadSheet
import utils.log_util as logger

class TestMap_ensemble_gene_name(unittest.TestCase):
    def setUp(self):
        logger.init()

        self.input_df_good = pd.DataFrame([[1, 0],
                                           [0, 0],
                                           [1, 1]],
                                          index=['ENSG00000000003', "ENSG00000000457", 'ENSG00000000005'],
                                          columns=['a', 'b'])

        self.input_df_bad = pd.DataFrame([[1, 0],
                                          [0, 0],
                                          [1, 1]],
                                         index=['ENSG00000000003', "ENSG00000000457", 'S00000005'],
                                         columns=['a', 'b'])

        self.input_df_cannot_map = pd.DataFrame([[1, 0],
                                                   [0, 0],
                                                   [1, 1]],
                                                  index=['000000003', "000457", 'S00000005'],
                                                  columns=['a', 'b'])
        self.run_parameters = {
            "spreadsheet_name_full_path": "../data/spreadsheets/example.tsv",
            "results_directory": "./",
            "redis_credential": {
                "host": "knowredis.knoweng.org",
                "port": 6379,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606'
        }

        self.output_mapping = "./example_MAP.tsv"

        self.golden_output_good = pd.DataFrame([[1, 0],
                                                [0, 0],
                                                [1, 1]],
                                               index=['ENSG00000000003', "ENSG00000000457", 'ENSG00000000005'],
                                               columns=['a', 'b'])

    def tearDown(self):
        del self.input_df_good
        del self.input_df_bad
        del self.input_df_cannot_map
        del self.run_parameters
        del self.golden_output_good

    def test_map_ensemble_gene_name_empty_mapped(self):
        ret_df_mapped_dedup, map_filtered_dedup, mapping = SpreadSheet.map_ensemble_gene_name(self.input_df_cannot_map,
                                                                                              self.run_parameters)
        ret_val_boolean = True if ret_df_mapped_dedup is not None else False
        self.assertEqual(False, ret_val_boolean)

    '''
    def test_map_ensemble_gene_name_good(self):
        ret_df_mapped_dedup, map_filtered_dedup, mapping = SpreadSheet.map_ensemble_gene_name(self.input_df_good, self.run_parameters)
        ret_val_boolean = True if ret_df_mapped_dedup is not None else False
        self.assertEqual(True, ret_val_boolean)
        npytest.assert_array_equal(self.golden_output_good, ret_df_mapped_dedup)



    def test_map_ensemble_gene_name_bad(self):
        ret_df_mapped_dedup, map_filtered_dedup, mapping = SpreadSheet.map_ensemble_gene_name(self.input_df_bad, self.run_parameters)
        ret_val_boolean = True if ret_df_mapped_dedup is not None else False
        self.assertEqual(True, ret_val_boolean)
    '''

if __name__ == '__main__':
    unittest.main()
