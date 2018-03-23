import unittest
import os
import data_cleanup_toolbox as data_cln

class TestRun_geneset_characterization_pipeline(unittest.TestCase):
    def setUp(self):
        self.run_parameters = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression_binary.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "impute": "average",
            "redis_credential": {
                "host": "knowredis.knoweng.org",
                "port": 6379,
                "password": "KnowEnG"
            },
            "pipeline_type": "geneset_characterization_pipeline"
        }

        self.file_ETL = "TEST_1_gene_expression_binary_ETL.tsv"
        self.file_MAP = "TEST_1_gene_expression_binary_MAP.tsv"
        self.file_UNMAPPED = "TEST_1_gene_expression_binary_User_To_Ensembl.tsv"

    def tearDown(self):
        del self.run_parameters
        del self.file_ETL
        del self.file_MAP
        del self.file_UNMAPPED

    def test_run_geneset_characterization_pipeline(self):
        ret_flag, ret_msg = data_cln.run_geneset_characterization_pipeline(self.run_parameters)
        self.assertEqual(True, ret_flag)
        os.remove(self.file_UNMAPPED)
        os.remove(self.file_MAP)
        os.remove(self.file_ETL)


if __name__ == '__main__':
    unittest.main()
