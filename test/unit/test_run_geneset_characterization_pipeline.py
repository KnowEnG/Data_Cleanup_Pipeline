import unittest
import data_cleanup_toolbox as data_cln
import os

class TestRun_geneset_characterization_pipeline(unittest.TestCase):
    def setUp(self):
        self.run_parameters = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression_binary.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "redis_credential": {
                "host": "knowredis.knowhub.org",
                "port": 6380,
                "password": "KnowEnG"
            },
            "pipeline_type": "geneset_characterization_pipeline"
        }

        self.file_ETL = "TEST_1_gene_expression_binary_ETL.tsv"
        self.file_MAP = "TEST_1_gene_expression_binary_MAP.tsv"
        self.file_UNMAPPED = "TEST_1_gene_expression_binary_UNMAPPED.tsv"

    def tearDown(self):
        del self.run_parameters
        os.remove(self.file_ETL)
        os.remove(self.file_MAP)
        os.remove(self.file_UNMAPPED)


    def test_run_geneset_characterization_pipeline(self):
        ret_flag, ret_msg = data_cln.run_geneset_characterization_pipeline(self.run_parameters)
        self.assertEqual(True, ret_flag)


if __name__ == '__main__':
    unittest.main()
