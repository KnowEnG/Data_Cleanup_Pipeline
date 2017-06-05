import unittest
import data_cleanup_toolbox as data_cln
import os

class TestRun_gene_prioritization_pipeline(unittest.TestCase):
    def setUp(self):
        self.run_parameters = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression_real_value.tsv",
            "phenotype_name_full_path": "../../data/spreadsheets/TEST_1_phenotype_pearson.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "samples_clustering_pipeline",
            "correlation_measure": 'pearson',
            "redis_credential": {
                "host": "knowredis.knowhub.org",
                "port": 6380,
                "password": "KnowEnG"
            }
        }
        self.file_ETL = "TEST_1_gene_expression_real_value_ETL.tsv"
        self.file_MAP = "TEST_1_gene_expression_real_value_MAP.tsv"
        self.file_UNMAPPED = "TEST_1_gene_expression_real_value_UNMAPPED.tsv"
        self.phenotype_ETL = "TEST_1_phenotype_pearson_ETL.tsv"


    def tearDown(self):
        del self.run_parameters
        os.remove(self.file_ETL)
        os.remove(self.file_MAP)
        os.remove(self.file_UNMAPPED)
        os.remove(self.phenotype_ETL)


    def test_run_gene_prioritization_pipeline(self):
        ret_flag, ret_msg = data_cln.run_gene_prioritization_pipeline(self.run_parameters)
        self.assertEqual(True, ret_flag)


if __name__ == '__main__':
    unittest.main()
