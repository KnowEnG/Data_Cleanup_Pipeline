import unittest
import os
import pandas as pd
import numpy.testing as npytest
import data_cleanup_toolbox as data_cln


class TestSanity_check_data_file(unittest.TestCase):
    def setUp(self):
        self.input_df_good = pd.DataFrame([[1, 0],
                                           [0, 0],
                                           [1, 1]],
                                          index=['ENSG00000000003', "ENSG00000000457", 'ENSG00000000005'],
                                          columns=['a', 'b']
                                          )

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

        self.output_ensemble = "./example_ETL.tsv"
        self.output_mapped = "./example_MAP.tsv"

    def tearDown(self):
        del self.input_df_good
        del self.run_parameters
        os.remove(self.output_mapped)
        os.remove(self.output_ensemble)
        del self.output_mapped
        del self.output_ensemble

    def test_sanity_check_data_file(self):
        ret_val, ret_msg = data_cln.sanity_check_data_file(self.input_df_good, self.run_parameters)
        self.assertEqual(True, ret_val)


if __name__ == '__main__':
    unittest.main()
