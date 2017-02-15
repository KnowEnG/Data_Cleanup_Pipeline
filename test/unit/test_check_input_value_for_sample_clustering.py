import unittest
import pandas as pd
import data_cleanup_toolbox as data_cln


class TestCheck_input_value_for_samples_clustering(unittest.TestCase):
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

        self.run_parameters_sc = {
            "spreadsheet_name_full_path": "../data/spreadsheets/example.tsv",
            "phenotype_full_path": "../data/spreadsheets/phenotype.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "samples_clustering_pipeline"
        }
        self.run_parameters_no_phenotype = {
            "spreadsheet_name_full_path": "../data/spreadsheets/example.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "samples_clustering_pipeline"
        }

    def tearDown(self):
        del self.input_df
        del self.input_phenotype_df
        del self.input_df_nan
        del self.run_parameters_sc

    def test_check_input_value_for_samples_clustering(self):
        ret_df, ret_msg = data_cln.check_input_value_for_samples_clustering(self.input_df, self.run_parameters_sc, self.input_phenotype_df)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_nan_input_value_in_spreadsheet(self):
        ret_df, ret_msg = data_cln.check_input_value_for_samples_clustering(self.input_df_nan, self.run_parameters_sc, self.input_phenotype_df)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_check_text_input_value_in_spreadsheet(self):
        ret_df, ret_msg = data_cln.check_input_value_for_samples_clustering(self.input_df_text, self.run_parameters_sc, self.input_phenotype_df)
        ret_flag = ret_df is not None
        self.assertEqual(False, ret_flag)

    def test_check_nan_phenotype_argument(self):
        ret_df, ret_msg = data_cln.check_input_value_for_samples_clustering(self.input_df, self.run_parameters_sc)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    def test_check_nan_phenotype_argument_test2(self):
        ret_df, ret_msg = data_cln.check_input_value_for_samples_clustering(self.input_df, self.run_parameters_sc)
        ret_flag = ret_df is not None
        self.assertEqual(True, ret_flag)

    

