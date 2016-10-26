import unittest
import os
import pandas as pd
import numpy.testing as npytest
import data_cleanup_toolbox as data_cln


class TestCheck_ensemble_gene_name(unittest.TestCase):
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
                "host": "knownbs.dyndns.org",
                "port": 6380,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606'
        }
        self.output_processed = "./example_ETL.tsv"
        self.output_mapping = "./example_MAP.tsv"

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
        os.remove(self.output_processed)
        os.remove(self.output_mapping)
        del self.output_processed
        del self.output_mapping

    def test_check_ensemble_gene_name_good(self):
        ret_val, ret_msg = data_cln.check_ensemble_gene_name(self.input_df_good, self.run_parameters)
        self.assertEqual(True, ret_val)

        file_content_df = pd.read_csv(self.output_processed, sep='\t', header=0, index_col=0, mangle_dupe_cols=False)
        npytest.assert_array_equal(self.golden_output_good, file_content_df)

    def test_check_ensemble_gene_name_empty_mapped(self):
        ret_val, ret_msg = data_cln.check_ensemble_gene_name(self.input_df_empty_mapped, self.run_parameters)
        self.assertEqual(False, ret_val)

    def test_check_ensemble_gene_name_bad(self):
        ret_val, ret_msg = data_cln.check_ensemble_gene_name(self.input_df_bad, self.run_parameters)
        self.assertEqual(True, ret_val)


if __name__ == '__main__':
    unittest.main()
