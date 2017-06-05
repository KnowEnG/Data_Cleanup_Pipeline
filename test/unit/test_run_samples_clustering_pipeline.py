import unittest
import data_cleanup_toolbox as data_cln
import os

class TestRun_samples_clustering_pipeline(unittest.TestCase):
    def setUp(self):
        self.run_parameters = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression_real_value.tsv",
            "gg_network_name_full_path":  "../../data/networks/TEST_1_gene_gene.edge",
            "results_directory": "./",
            "redis_credential": {
                "host": "knowredis.knowhub.org",
                "port": 6380,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "samples_clustering_pipeline"
        }

        self.run_parameters_no_phenotype = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression_real_value.tsv",
            "gg_network_name_full_path": "../../data/networks/TEST_1_gene_gene.edge",
            "phenotype_name_full_path": "",
            "results_directory": "./",
            "redis_credential": {
                "host": "knowredis.knowhub.org",
                "port": 6380,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "samples_clustering_pipeline"
        }

        self.file_ETL = "TEST_1_gene_expression_real_value_ETL.tsv"
        self.file_MAP = "TEST_1_gene_expression_real_value_MAP.tsv"
        self.file_UNMAPPED = "TEST_1_gene_expression_real_value_UNMAPPED.tsv"

    def tearDown(self):
        del self.run_parameters

    def test_run_samples_clustering_pipeline(self):
        ret_flag, ret_msg = data_cln.run_samples_clustering_pipeline(self.run_parameters)
        self.assertEqual(True, ret_flag)
        os.remove(self.file_ETL)
        os.remove(self.file_MAP)
        os.remove(self.file_UNMAPPED)

    def test_run_samples_clustering_pipeline_no_phenotype(self):
        ret_flag, ret_msg = data_cln.run_samples_clustering_pipeline(self.run_parameters_no_phenotype)
        self.assertEqual(False, ret_flag)


if __name__ == '__main__':
    unittest.main()
