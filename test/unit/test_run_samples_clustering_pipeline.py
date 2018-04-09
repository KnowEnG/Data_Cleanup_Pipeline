import unittest
import data_cleanup_toolbox as data_cln
import os

class TestRun_samples_clustering_pipeline(unittest.TestCase):
    def setUp(self):
        self.run_parameters = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression_positive_real_number.tsv",
            "gg_network_name_full_path":  "../../data/networks/TEST_1_gene_gene.edge",
            "results_directory": "./",
            "redis_credential": {
                "host": "knowredis.knoweng.org",
                "port": 6379,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "samples_clustering_pipeline"
        }

        self.run_parameters_empty_phenotype = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression_positive_real_number.tsv",
            "gg_network_name_full_path": "../../data/networks/TEST_1_gene_gene.edge",
            "phenotype_name_full_path": "",
            "results_directory": "./",
            "redis_credential": {
                "host": "knowredis.knoweng.org",
                "port": 6379,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "samples_clustering_pipeline"
        }

        self.file_ETL = "TEST_1_gene_expression_positive_real_number_ETL.tsv"
        self.file_MAP = "TEST_1_gene_expression_positive_real_number_MAP.tsv"
        self.file_UNMAPPED = "TEST_1_gene_expression_positive_real_number_User_To_Ensembl.tsv"

    def tearDown(self):
        del self.run_parameters

    def test_run_samples_clustering_pipeline(self):
        ret_flag, ret_msg = data_cln.run_samples_clustering_pipeline(self.run_parameters)
        self.assertEqual(True, ret_flag)
        os.remove(self.file_ETL)
        os.remove(self.file_MAP)
        os.remove(self.file_UNMAPPED)

    def test_run_samples_clustering_pipeline_no_phenotype(self):
        ret_flag, ret_msg = data_cln.run_samples_clustering_pipeline(self.run_parameters_empty_phenotype)
        self.assertEqual(False, ret_flag)


if __name__ == '__main__':
    unittest.main()
