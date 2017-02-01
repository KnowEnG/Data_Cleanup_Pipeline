import unittest
import data_cleanup_toolbox as data_cln
import os

class TestRun_gene_priorization_pipeline(unittest.TestCase):
    def setUp(self):
        self.run_parameters = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression.tsv",
            "phenotype_full_path": "../../data/spreadsheets/TEST_1_phenotype.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "sample_clustering_pipeline",
            "redis_credential": {
                "host": "knownet.knowhub.org",
                "port": 6379,
                "password": "KnowEnG"
            }
        }
        self.file_ETL = "TEST_1_gene_expression_ETL.tsv"
        self.file_MAP = "TEST_1_gene_expression_MAP.tsv"
        self.file_UNMAPPED = "TEST_1_gene_expression_UNMAPPED.tsv"
        self.phenotype_ETL = "TEST_1_phenotype_ETL.tsv"


    def tearDown(self):
        del self.run_parameters
        os.remove(self.file_ETL)
        os.remove(self.file_MAP)
        os.remove(self.file_UNMAPPED)
        os.remove(self.phenotype_ETL)


    def test_run_gene_priorization_pipeline(self):
        ret_flag, ret_msg = data_cln.run_gene_priorization_pipeline(self.run_parameters)
        self.assertEqual(True, ret_flag)


if __name__ == '__main__':
    unittest.main()