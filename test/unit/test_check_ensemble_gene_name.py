import unittest
import os
import pandas as pd
import numpy.testing as npytest
import input_data_cleaning as data_cln

class TestCheck_ensemble_gene_name(unittest.TestCase):
    def setUp(self):
        self.input_df_good = pd.DataFrame([["ENSG00000000003", 1, 0],
                                      ["ENSG00000000457", 0, 0],
                                      ["ENSG00000000005", 1, 1]])
        self.input_df_bad = pd.DataFrame([["ENSG00000000003", 1, 0],
                                           ["ENSG00000000457", 0, 0],
                                           ["S00000005", 1, 1]])

        self.input_df_empty_mapped = pd.DataFrame([["000000003", 1, 0],
                                           ["000457", 0, 0],
                                           ["S00000005", 1, 1]])
        self.run_parameters = {
            "results_directory": "./",
            "redis_credential": {
                "host": "knownbs.dyndns.org",
                "port": 6380,
                "password": "KnowEnG"
            },
        }
        self.output_mapped = "./tmp_mapped"
        self.output_unmapped_one = "./tmp_unmapped_one"
        self.output_unmapped_many = "./tmp_unmapped_many"

        self.golden_output_good = pd.DataFrame([["ENSG00000000003", 1, 0, "ENSG00000000003"],
                                      ["ENSG00000000457", 0, 0, "ENSG00000000457"],
                                      ["ENSG00000000005", 1, 1, "ENSG00000000005"]])
        self.golden_output_unmapped_one = pd.DataFrame([["S00000005", 1, 1, "unmapped-none"]])

    def tearDown(self):
        del self.input_df_good
        del self.input_df_bad
        del self.run_parameters
        del self.golden_output_good
        os.remove(self.output_mapped)
        os.remove(self.output_unmapped_one)
        os.remove(self.output_unmapped_many)
        del self.output_mapped
        del self.output_unmapped_one
        del self.output_unmapped_many

    def test_check_ensemble_gene_name_good(self):
        ret_val, ret_msg = data_cln.check_ensemble_gene_name(self.input_df_good, self.run_parameters)
        self.assertEqual(True, ret_val)

        file_content_df = pd.read_csv(self.output_mapped, sep='\t', header=None)
        npytest.assert_array_equal(self.golden_output_good, file_content_df)


    def test_check_ensemble_gene_name_bad(self):
        ret_val, ret_msg = data_cln.check_ensemble_gene_name(self.input_df_bad, self.run_parameters)
        self.assertEqual(False, ret_val)

        file_content_df = pd.read_csv(self.output_unmapped_one, sep="\t", header=None)
        npytest.assert_array_equal(self.golden_output_unmapped_one, file_content_df)

    def test_check_ensemble_gene_name_empty_mapped(self):
        ret_val, ret_msg = data_cln.check_ensemble_gene_name(self.input_df_empty_mapped, self.run_parameters)
        self.assertEqual(False, ret_val)


if __name__ == '__main__':
    unittest.main()