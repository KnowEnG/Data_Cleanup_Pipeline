import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln
import os


class TestCheck_input_value_for_sample_clustering(unittest.TestCase):
    def setUp(self):
        self.input_df = pd.DataFrame([[1, 2],
                                      [0, -10],
                                      [1, 9]],
                                     index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                     columns=['a', 'b'])
        self.input_df_nan = pd.DataFrame([[1, 0],
                                          [0, None],
                                          [1, 1]],
                                         index=['ENSG00001027003', "ENSG00001027003", 'ENSG00008000303'],
                                         columns=['a', 'b'])
        self.input_df_text = pd.DataFrame([["text", 0],
                                          [0, "text"],
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

        self.run_parameters_gp = {
            "spreadsheet_name_full_path": "../data/spreadsheets/example.tsv",
            "phenotype_full_path": "../data/spreadsheets/phenotype.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "sample_clustering_pipeline"
        }

        self.data_type = "user_spreadsheet"
        self.phenotype_output = "./phenotype_ETL.tsv"

    def tearDown(self):
        del self.input_df
        del self.input_phenotype_df
        del self.input_df_nan
        del self.run_parameters_gp
        del self.data_type
        del self.phenotype_output

    def test_check_input_value_for_sample_clustering(self):
        ret_df, ret_msg = data_cln.check_input_value_for_sample_clustering(self.input_df, self.input_phenotype_df,
                                                                           self.run_parameters_gp)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)
        os.remove(self.phenotype_output)

    def test_check_nan_input_value_in_spreadsheet(self):
        ret_df, ret_msg = data_cln.check_input_value_for_sample_clustering(self.input_df_nan, self.input_phenotype_df,
                                                                           self.run_parameters_gp)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_check_text_input_value_in_spreadsheet(self):
        ret_df, ret_msg = data_cln.check_input_value_for_sample_clustering(self.input_df_text, self.input_phenotype_df,
                                                                           self.run_parameters_gp)

        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)