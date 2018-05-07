import unittest
import os
import pandas as pd
import numpy.testing as npytest
from utils.gene_mapping_util import GeneMappingUtil
import utils.log_util as logger

class TestMap_ensemble_gene_name(unittest.TestCase):
    def setUp(self):
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

        self.input_df_empty_mapped = pd.DataFrame([[1, 0],
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
        self.output_unmapped = "./example_User_To_Ensembl.tsv"

        self.golden_output_good = pd.DataFrame([[1, 0],
                                                [0, 0],
                                                [1, 1]],
                                               index=['ENSG00000000003', "ENSG00000000457", 'ENSG00000000005'],
                                               columns=['a', 'b'])

    def tearDown(self):
        del self.input_df_good
        del self.input_df_bad
        del self.input_df_empty_mapped
        del self.run_parameters
        del self.golden_output_good
        del self.output_mapping
        del self.output_unmapped

    def test_map_ensemble_gene_name_good(self):
        ret_val = GeneMappingUtil.map_ensemble_gene_name(self.input_df_good, self.run_parameters)
        ret_val_boolean = True if ret_val is not None else False
        self.assertEqual(True, ret_val_boolean)
        npytest.assert_array_equal(self.golden_output_good, ret_val)

        # clean up files
        os.remove(self.output_mapping)
        os.remove(self.output_unmapped)

    def test_map_ensemble_gene_name_empty_mapped(self):
        ret_val = GeneMappingUtil.map_ensemble_gene_name(self.input_df_empty_mapped, self.run_parameters)
        ret_val_boolean = True if ret_val is not None else False
        self.assertEqual(False, ret_val_boolean)

    def test_map_ensemble_gene_name_bad(self):
        ret_val = GeneMappingUtil.map_ensemble_gene_name(self.input_df_bad, self.run_parameters)
        ret_val_boolean = True if ret_val is not None else False
        self.assertEqual(True, ret_val_boolean)

        # clean up files
        os.remove(self.output_mapping)
        os.remove(self.output_unmapped)


if __name__ == '__main__':
    logger.init()
    unittest.main()
