import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln
import os


class TestCheck_user_spreadsheet_value(unittest.TestCase):
    def setUp(self):
        self.input_df = pd.DataFrame([[1, 0],
                                      [0, 0],
                                      [1, 1]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])
        self.input_nan_df = pd.DataFrame([[1, 0],
                                          [0, None],
                                          [1, 1]],
                                         index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                         columns=['a', 'b'])

        self.input_phenotype_df = pd.DataFrame(
            [[1.1, 2.2, 3.3]],
            index=['drug1'],
            columns=['a', 'b', 'c']
        )

        self.input_phenotype_df_bad = pd.DataFrame(
            [[1.1, 2.2, 3.3]],
            index=['drug1'],
            columns=['d', 'e', 'f']
        )

        self.run_parameters = {
            "spreadsheet_name_full_path": "../data/spreadsheets/example.tsv",
            "phenotype_full_path": ".. / data / spreadsheets / phenotype.tsv",
            "results_directory": "./",
            "redis_credential": {
                "host": "knownbs.dyndns.org",
                "port": 6380,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606'
        }

        self.pipeline_sc = "sample_clustering_pipeline"
        self.data_type = "user_spreadsheet"
        self.phenotype_output = "./phenotype_ETL.tsv"

    def tearDown(self):
        del self.input_df
        del self.input_phenotype_df
        del self.input_nan_df
        del self.run_parameters
        del self.pipeline_sc
        del self.data_type
        del self.phenotype_output

    def test_check_user_spreadsheet_value_pass(self):
        ret_df, ret_msg = data_cln.check_user_spreadsheet_value(self.input_df, self.input_phenotype_df,
                                                                "sample_clustering_pipeline", "user_spreadsheet", self.run_parameters)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)


    def test_check_user_spreadsheet_value_Nan_value(self):
        ret_df, ret_msg = data_cln.check_user_spreadsheet_value(self.input_nan_df, self.input_phenotype_df,
                                                                "sample_clustering_pipeline", "user_spreadsheet", self.run_parameters)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)


    def test_check_user_spreadsheet_value_case_a(self):
        ret_df, ret_msg = data_cln.check_user_spreadsheet_value(self.input_df, self.input_phenotype_df,
                                                                "sample_clustering_pipeline", "user_spreadsheet", self.run_parameters)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)


    def test_check_user_spreadsheet_value_case_b(self):
        ret_df, ret_msg = data_cln.check_user_spreadsheet_value(self.input_df, self.input_phenotype_df,
                                                                "gene_priorization_pipeline", "", self.run_parameters)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)
        os.remove(self.phenotype_output)

    def test_check_user_spreadsheet_value_case_c(self):
        ret_df, ret_msg = data_cln.check_user_spreadsheet_value(self.input_df, self.input_phenotype_df_bad,
                                                                "gene_priorization_pipeline", "", self.run_parameters)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)


if __name__ == '__main__':
    unittest.main()
